# -*- coding: utf-8 -*-
# Python 2.7 / 3.x 兼容：支持全路径的交互式压缩文件搜索程序
# 适配场景：服务器无图形界面（SSH/纯命令行）、无DISPLAY环境变量
# 功能：保留菜单交互 + 支持全路径文件/目录输入
# 零依赖：仅使用 Python 内置库（tarfile），跨平台不依赖外部 tar 命令

from __future__ import print_function

import os
import sys
import io
import tarfile
import tempfile
import shutil
import multiprocessing

try:
    # Py2
    input_func = raw_input
except NameError:
    # Py3
    input_func = input

# 内存保护配置
MAX_LINE_BUFFER = 1024 * 1024  # 每行最大读取1MB
SKIP_LARGE_FILE_SIZE = 10 * 1024 * 1024 * 1024  # 跳过10GB以上文件
DEFAULT_SAVE_PATH = "matched_files.txt"
DISPLAY_PATH_WIDTH = 50  # 路径显示宽度（超长则截断）


def _to_utf8(s):
    """将输入转为 UTF-8 字节串，用于搜索"""
    if s is None:
        return b""
    if isinstance(s, bytes):
        try:
            return s.decode("utf-8").encode("utf-8")
        except Exception:
            return s
    try:
        return s.encode("utf-8")
    except Exception:
        return b""


def _get_tar_mode(file_path):
    """根据扩展名返回 tarfile 打开模式，不支持则返回 None"""
    lower = file_path.lower()
    if lower.endswith(".tar.gz") or lower.endswith(".gz"):
        return "r:gz"
    if lower.endswith(".tar"):
        return "r:"
    return None


def _is_supported_archive(file_path):
    """是否为支持的压缩格式"""
    return _get_tar_mode(file_path) is not None


def _truncate_display(s, width=DISPLAY_PATH_WIDTH):
    """截断过长路径用于显示"""
    s = s or ""
    if len(s) <= width:
        return s
    return s[: width - 3] + "..."


def print_menu():
    """打印交互菜单（美化版）"""
    print("\n" + "=" * 70)
    print("                    压缩文件字符串搜索工具（命令行版）")
    print("=" * 70)
    print("1. 搜索指定路径下所有.tar.gz/.tar/.gz文件（支持全路径）")
    print("2. 搜索指定单个压缩文件（支持全路径文件名）")
    print("3. 退出程序")
    print("=" * 70)


def get_file_size(file_path):
    """获取文件大小（字节）"""
    try:
        return os.path.getsize(file_path)
    except Exception as e:
        print("⚠️ 获取文件大小失败 %s：%s" % (file_path, str(e)))
        return 0


def count_str_in_tarfile(archive_path, search_bytes):
    """
    使用 tarfile 在单个压缩包内搜索，统计包含关键字的行数。
    跨平台，不依赖外部 tar 命令。
    返回 (匹配行数, 错误信息)，成功时错误信息为空。
    """
    mode = _get_tar_mode(archive_path)
    if not mode:
        return -1, "不支持的格式"

    try:
        tf = tarfile.open(archive_path, mode)
    except Exception as e:
        return -1, str(e)

    match_count = 0
    try:
        for member in tf.getmembers():
            if not member.isfile():
                continue

            # 跳过超大成员（如误打包的大文件）
            if member.size > SKIP_LARGE_FILE_SIZE:
                continue

            try:
                f = tf.extractfile(member)
            except Exception:
                continue

            if f is None:
                continue

            try:
                while True:
                    line = f.readline(MAX_LINE_BUFFER)
                    if not line:
                        break
                    if search_bytes in line:
                        match_count += 1
            except Exception:
                pass
            finally:
                try:
                    f.close()
                except Exception:
                    pass

        tf.close()
        return match_count, ""
    except Exception as e:
        try:
            tf.close()
        except Exception:
            pass
        return -1, str(e)


def count_str_in_compressed_file(compressed_file, search_str):
    """统计单个压缩文件的匹配行数"""
    compressed_file = os.path.abspath(compressed_file)
    if not os.path.exists(compressed_file):
        print("❌ %s：文件不存在" % compressed_file)
        return -1

    file_size = get_file_size(compressed_file)
    if file_size > SKIP_LARGE_FILE_SIZE:
        print("⚠️ %s：文件大小超过%dGB，跳过搜索" % (
            compressed_file, SKIP_LARGE_FILE_SIZE // (1024 * 1024 * 1024)
        ))
        return -1

    if not _is_supported_archive(compressed_file):
        print("⚠️ %s：非tar.gz/tar/gz格式，跳过处理" % compressed_file)
        return -1

    search_bytes = _to_utf8(search_str)
    if not search_bytes:
        return -1

    cnt, err = count_str_in_tarfile(compressed_file, search_bytes)
    if err:
        # tarfile 打开/读取失败时，尝试解压到临时目录再搜索（备用方案）
        print("🔧 %s：直接读取失败，尝试分步解压 - %s" % (compressed_file, err))
        return _count_via_temp_extract(compressed_file, search_bytes)

    return cnt


def _count_via_temp_extract(compressed_file, search_bytes):
    """备用方案：解压到临时目录后逐文件搜索"""
    temp_dir = None
    try:
        temp_dir = tempfile.mkdtemp(prefix="tar_temp_")
        mode = _get_tar_mode(compressed_file)
        tf = tarfile.open(compressed_file, mode)

        try:
            tf.extractall(temp_dir)
        except Exception as e:
            print("❌ %s：分步解压失败 - %s" % (compressed_file, str(e)))
            return -1
        finally:
            tf.close()

        total_count = 0
        for root, dirs, files in os.walk(temp_dir):
            for name in files:
                file_path = os.path.join(root, name)
                if get_file_size(file_path) > SKIP_LARGE_FILE_SIZE:
                    continue

                try:
                    with open(file_path, "rb") as f:
                        while True:
                            line = f.readline(MAX_LINE_BUFFER)
                            if not line:
                                break
                            if search_bytes in line:
                                total_count += 1
                except Exception:
                    continue

        return total_count

    except Exception as e:
        print("❌ %s：分步处理异常 - %s" % (compressed_file, str(e)))
        return -1
    finally:
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except Exception:
                pass


def save_matched_files(matched_files, target, search_str, save_path=DEFAULT_SAVE_PATH):
    """保存匹配结果到文件（UTF-8 编码）"""
    try:
        with io.open(save_path, "w", encoding="utf-8") as f:
            f.write(u"搜索范围：%s\n" % target)
            f.write(u"匹配字符串：%s\n" % search_str)
            f.write(u"匹配行数>0的压缩文件列表：\n")
            f.write(u"文件名\t匹配行数\n")
            f.write(u"-" * 60 + "\n")
            for file_name, count in matched_files:
                f.write(u"%s\t%d\n" % (file_name, count))
            f.write(u"\n总计匹配行数：%d\n" % sum(c for _, c in matched_files))
        print("\n✅ 匹配结果已保存到：%s" % save_path)
    except Exception as e:
        print("❌ 保存结果失败：%s" % str(e))


def search_specified_path(search_str):
    """搜索指定路径下所有压缩文件（支持全路径）"""
    while True:
        path = input_func("\n请输入要搜索的路径（如：/data/logs/ 或 ./test/）：").strip()
        if not path:
            print("❌ 路径不能为空！")
            continue
        if not os.path.isdir(path):
            print("❌ %s：不是有效的目录路径，请重新输入！" % path)
            continue
        break

    abs_path = os.path.abspath(path)
    print("\n🔍 开始搜索路径%s下所有压缩文件，匹配字符串：%s" % (abs_path, search_str))
    print("=" * 60)

    compressed_files = []
    for root, dirs, files in os.walk(abs_path):
        for name in files:
            file_path = os.path.join(root, name)
            if _is_supported_archive(file_path):
                compressed_files.append(file_path)

    if not compressed_files:
        print("💡 %s：未找到任何tar.gz/tar/gz文件" % abs_path)
        return [], abs_path

    total_count = 0
    matched_files = []

    use_parallel = False
    if len(compressed_files) >= 2:
        choice = input_func("找到 %d 个文件，是否使用并行搜索？(y/n) [n]：" % len(compressed_files)).strip().lower()
        use_parallel = choice in ("y", "yes", "1", "true")

    if use_parallel:
        print("🔍 正在并行处理 %d 个文件..." % len(compressed_files))
        try:
            import search_v2_worker
            n_workers = min(len(compressed_files), multiprocessing.cpu_count() or 4)
            pool = multiprocessing.Pool(processes=n_workers)
            try:
                args = [(path, search_str) for path in compressed_files]
                results = pool.map(search_v2_worker.worker_count_file, args)
            finally:
                pool.close()
                pool.join()
        except Exception as e:
            use_parallel = False
            results = None
            print("⚠️ 并行搜索失败，改用顺序搜索：%s" % str(e))

    if use_parallel and results is not None:
        for comp_file, cnt, msgs in results:
            for msg in (msgs or []):
                print(msg)
            if cnt >= 0:
                total_count += cnt
                disp = _truncate_display(comp_file)
                print("📄 %-50s 匹配行数：%d" % (disp, cnt))
                if cnt > 0:
                    matched_files.append((comp_file, cnt))
    else:
        for comp_file in compressed_files:
            print("🔍 正在处理：%s" % comp_file)
            cnt = count_str_in_compressed_file(comp_file, search_str)
            if cnt >= 0:
                total_count += cnt
                disp = _truncate_display(comp_file)
                print("📄 %-50s 匹配行数：%d" % (disp, cnt))
                if cnt > 0:
                    matched_files.append((comp_file, cnt))

    print("=" * 60)
    print("✅ 搜索完成！")
    print("📊 总计匹配行数：%d" % total_count)
    print("📊 匹配行数>0的文件数：%d" % len(matched_files))

    if matched_files:
        print("\n📋 匹配行数>0的文件列表：")
        for idx, (file_name, count) in enumerate(matched_files, 1):
            print("  %d. %s → 匹配行数：%d" % (idx, file_name, count))

    return matched_files, abs_path


def search_specified_file(search_str):
    """搜索指定单个压缩文件（支持全路径）"""
    while True:
        file_name = input_func("\n请输入要搜索的压缩文件全路径（如：/data/logs/test.tar.gz）：").strip()
        if not file_name:
            print("❌ 文件名不能为空！")
            continue
        if not os.path.exists(file_name):
            print("❌ %s：文件不存在，请重新输入！" % file_name)
            continue
        if not _is_supported_archive(file_name):
            choice = input_func("⚠️ 非tar.gz/tar/gz格式，是否继续搜索？(y/n)：").strip().lower()
            if choice != "y":
                continue
        break

    abs_file = os.path.abspath(file_name)
    print("\n🔍 开始搜索指定文件：%s，匹配字符串：%s" % (abs_file, search_str))
    print("=" * 60)

    total_count = 0
    matched_files = []

    cnt = count_str_in_compressed_file(abs_file, search_str)
    if cnt >= 0:
        total_count = cnt
        disp = _truncate_display(abs_file)
        print("📄 %-50s 匹配行数：%d" % (disp, cnt))
        if cnt > 0:
            matched_files.append((abs_file, cnt))

    print("=" * 60)
    print("✅ 搜索完成！")
    print("📊 总计匹配行数：%d" % total_count)

    return matched_files, abs_file


def main():
    """主交互逻辑"""
    if sys.version_info[0] == 2:
        try:
            reload(sys)
            sys.setdefaultencoding("utf-8")
        except NameError:
            pass

    print("欢迎使用压缩文件字符串搜索工具（Python 2.7 / 3.x 命令行版）")
    print("💡 适配场景：无图形界面的SSH/纯命令行服务器")
    print("💡 支持全路径输入：绝对路径（/data/logs/）或相对路径（./test/）")
    print("💡 跨平台：使用内置 tarfile，无需安装 tar 命令\n")

    while True:
        print_menu()

        try:
            choice = int(input_func("\n请输入操作编号（1/2/3）：").strip())
        except (ValueError, TypeError):
            print("❌ 输入无效，请输入数字1/2/3！")
            continue

        if choice == 3:
            print("\n👋 退出程序，再见！")
            sys.exit(0)

        search_str = input_func("\n请输入要匹配的字符串：").strip()
        if not search_str:
            print("❌ 匹配字符串不能为空！")
            continue

        if choice == 1:
            matched_files, target = search_specified_path(search_str)
        elif choice == 2:
            matched_files, target = search_specified_file(search_str)
        else:
            print("❌ 无效的操作编号，请重新选择！")
            continue

        if matched_files:
            save_choice = input_func("\n是否保存匹配结果到文件？(y/n)：").strip().lower()
            if save_choice == "y":
                save_matched_files(matched_files, target, search_str)
        else:
            print("\n💡 无匹配行数>0的文件，无需保存")

        continue_choice = input_func("\n是否继续操作？(y/n)：").strip().lower()
        if continue_choice != "y":
            print("\n👋 退出程序，再见！")
            sys.exit(0)


if __name__ == "__main__":
    main()

