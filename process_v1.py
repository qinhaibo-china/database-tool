# -*- coding: utf-8 -*-
# Python 2.7 专用：仅处理指定压缩文件（解压→MySQL导入→查询→导出日志+清理临时文件）
# 使用方式1（命令行传参）：python process_file.py <压缩文件名/路径> <查找字符串>
# 使用方式2（交互模式）：python process_file.py
# 示例：python process_file.py 102447100-20260227-23-00.tar.gz '17693326'

import os
import sys
import subprocess
import glob
import shutil
import tempfile

# 配置MySQL连接信息（根据实际环境修改）
MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "admin",
    "db": "test",
    "port": 3306
}

# 清理配置（可根据需求调整）
CLEANUP_AFTER_IMPORT = True  # 导入成功后是否清理临时文件（True=清理，False=保留）
DECOMPRESS_DIR = "./decompressed"  # 解压目录（需清理的目录）

# 内存保护配置
MAX_LINE_BUFFER = 1024 * 1024  # 每行最大读取1MB

def print_usage():
    """打印使用说明"""
    print "用法错误！正确用法：%s <压缩文件名> <查找字符串>" % sys.argv[0]
    print "示例：%s 102447100-20260227-23-00.tar.gz '17693326'" % sys.argv[0]
    sys.exit(1)

def execute_shell_command(cmd):
    """执行shell命令，返回执行状态和输出"""
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = proc.communicate()
        return proc.returncode, stdout.strip(), stderr.strip()
    except Exception as e:
        return -1, "", str(e)

def safe_remove_file(file_path):
    """安全删除文件（处理文件不存在/权限问题）"""
    try:
        if os.path.exists(file_path) and os.path.isfile(file_path):
            os.remove(file_path)
            print "✅ 已清理临时文件：%s" % file_path
    except Exception as e:
        print "⚠️ 清理文件失败 %s：%s" % (file_path, str(e))

def safe_remove_dir(dir_path):
    """安全删除目录（处理目录不存在/权限问题）"""
    try:
        if os.path.exists(dir_path) and os.path.isdir(dir_path):
            shutil.rmtree(dir_path)
            print "✅ 已清理临时目录：%s" % dir_path
    except Exception as e:
        print "⚠️ 清理目录失败 %s：%s" % (dir_path, str(e))

def recursive_decompress(compressed_file, output_dir=DECOMPRESS_DIR):
    """递归解压嵌套压缩文件（.tar.gz → .tar → .sql）"""
    if not os.path.exists(compressed_file):
        print "❌ %s：文件不存在" % compressed_file
        return None
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 临时解压目录
    temp_dir = os.path.join(output_dir, "temp_%s" % os.path.basename(compressed_file).replace(".", "_"))
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    
    try:
        current_file = compressed_file
        # 第一步：解压.tar.gz到临时目录
        if current_file.endswith(".tar.gz") or current_file.endswith(".gz"):
            print "🔧 第一步解压：%s → 临时目录" % current_file
            cmd = ["tar", "zxvf", current_file, "-C", temp_dir]
            returncode, stdout, stderr = execute_shell_command(cmd)
            if returncode != 0:
                print "❌ 第一步解压失败：%s" % stderr
                shutil.rmtree(temp_dir)
                return None
            
            # 查找嵌套.tar文件
            tar_files = []
            for root, dirs, files in os.walk(temp_dir):
                for f in files:
                    if f.endswith(".tar"):
                        tar_files.append(os.path.join(root, f))
            
            if tar_files:
                current_file = tar_files[0]
                print "🔧 找到嵌套.tar文件：%s" % current_file
                
                # 第二步：解压.tar文件到最终目录
                print "🔧 第二步解压：%s → %s" % (current_file, output_dir)
                cmd = ["tar", "xvf", current_file, "-C", output_dir]
                returncode, stdout, stderr = execute_shell_command(cmd)
                if returncode != 0:
                    print "❌ 第二步解压失败：%s" % stderr
                    shutil.rmtree(temp_dir)
                    return None
            else:
                # 移动临时目录内容到最终目录
                for item in os.listdir(temp_dir):
                    src = os.path.join(temp_dir, item)
                    dst = os.path.join(output_dir, item)
                    if os.path.exists(dst):
                        if os.path.isdir(dst):
                            shutil.rmtree(dst)
                        else:
                            os.remove(dst)
                    if os.path.isdir(src):
                        shutil.move(src, dst)
                    else:
                        shutil.copy2(src, dst)
        
        # 查找.sql文件
        sql_files = []
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith(".sql"):
                    sql_files.append(os.path.join(root, f))
        
        if not sql_files:
            print "❌ %s：递归解压后未找到.sql文件" % compressed_file
            shutil.rmtree(temp_dir)
            return None
        
        # 返回第一个.sql文件
        sql_file = sql_files[0]
        if len(sql_files) > 1:
            print "⚠️ %s：解压后找到多个.sql文件，默认使用第一个：%s" % (compressed_file, sql_file)
        
        # 清理临时解压目录（仅清理temp子目录，保留.sql文件）
        shutil.rmtree(temp_dir)
        print "✅ 递归解压完成：%s → %s" % (compressed_file, sql_file)
        return sql_file
    
    except Exception as e:
        print "❌ 递归解压失败 %s：%s" % (compressed_file, str(e))
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        return None

def execute_mysql_command(sql_command, is_file_import=False, import_file_path=""):
    """执行MySQL命令（适配Python 2.7）"""
    mysql_cmd = [
        "mysql",
        "-h%s" % MYSQL_CONFIG["host"],
        "-u%s" % MYSQL_CONFIG["user"],
        "-p%s" % MYSQL_CONFIG["password"],
        "-P%d" % MYSQL_CONFIG["port"],
        MYSQL_CONFIG["db"]
    ]
    
    try:
        if is_file_import:
            # 导入SQL文件
            import_cmd = mysql_cmd + ["-e", "source %s;" % import_file_path]
            proc = subprocess.Popen(
                import_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            stdout, stderr = proc.communicate()
            if proc.returncode != 0:
                print "❌ MySQL导入文件失败：%s" % stderr.strip()
                return False
            print "✅ SQL文件导入成功：%s" % import_file_path
            
            # 导入成功后清理临时文件（可配置）
            if CLEANUP_AFTER_IMPORT:
                print "\n🔧 开始清理临时文件..."
                # 1. 清理导入的.sql文件
                safe_remove_file(import_file_path)
                # 2. 清理解压目录下剩余的所有文件/子目录
                safe_remove_dir(DECOMPRESS_DIR)
            
            return True
        else:
            # 执行普通SQL
            if "SELECT" in sql_command.upper():
                query_cmd = mysql_cmd + ["-e", sql_command]
                proc = subprocess.Popen(
                    query_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                stdout, stderr = proc.communicate()
                if proc.returncode != 0:
                    print "❌ MySQL查询失败：%s" % stderr.strip()
                    return False
                return stdout
            else:
                exec_cmd = mysql_cmd + ["-e", sql_command]
                proc = subprocess.Popen(
                    exec_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                stdout, stderr = proc.communicate()
                if proc.returncode != 0:
                    print "❌ MySQL执行失败：%s" % stderr.strip()
                    return False
                print "✅ SQL执行成功：%s" % sql_command[:50]
                return True
    except Exception as e:
        print "❌ MySQL操作异常：%s" % str(e)
        return False

def process_single_file(file_name, search_str):
    """处理单个文件：解压→清空表→导入→查询→导出结果"""
    print "\n🚀 开始处理文件：%s" % file_name
    
    # 1. 递归解压压缩文件
    sql_file = recursive_decompress(file_name)
    if not sql_file:
        return
    
    # 2. 清空test.general_log表
    truncate_sql = "truncate table test.general_log;"
    if not execute_mysql_command(truncate_sql):
        print "⚠️ 清空表失败，跳过后续处理：%s" % file_name
        # 清空表失败时，也清理临时文件（避免残留）
        if CLEANUP_AFTER_IMPORT:
            print "\n🔧 清空表失败，清理临时文件..."
            safe_remove_file(sql_file)
            safe_remove_dir(DECOMPRESS_DIR)
        return
    
    # 3. 导入解压后的SQL文件
    import_success = execute_mysql_command("", is_file_import=True, import_file_path=sql_file)
    if not import_success:
        print "⚠️ 导入文件失败，跳过后续处理：%s" % file_name
        # 导入失败时，清理临时文件
        if CLEANUP_AFTER_IMPORT:
            print "\n🔧 导入失败，清理临时文件..."
            safe_remove_file(sql_file)
            safe_remove_dir(DECOMPRESS_DIR)
        return
    
    # 4. 执行查询并导出结果
    query_sql = "SELECT event_time,user_host,CAST(argument AS CHAR) FROM test.general_log where CAST(argument AS CHAR) like '%%%s%%';" % search_str
    query_result = execute_mysql_command(query_sql)
    
    if query_result and query_result != True:
        # 分块保存查询结果到日志文件
        result_log = "%s_query_result.log" % os.path.splitext(file_name)[0]
        try:
            with open(result_log, "wb") as f:
                chunk_size = 1024 * 1024
                for i in range(0, len(query_result), chunk_size):
                    chunk = query_result[i:i+chunk_size]
                    f.write(chunk)
            print "✅ 查询结果已保存到：%s" % result_log
        except Exception as e:
            print "❌ 保存查询结果失败：%s" % str(e)
    elif query_result == False:
        print "❌ 查询执行失败：%s" % file_name
    else:
        print "💡 查询无结果返回：%s" % file_name
    
    print "\n✅ 文件处理完成！"

def get_input_interactive():
    """交互模式获取输入（仅新增功能，不影响原有逻辑）"""
    print "\n📋 未检测到命令行参数，进入交互模式："
    # 获取压缩文件路径/名称
    while True:
        file_path = raw_input("请输入压缩文件的名称/完整路径：").strip()
        if not file_path:
            print "❌ 文件名/路径不能为空！"
            continue
        if os.path.exists(file_path):
            break
        else:
            print "❌ 文件不存在，请检查后重新输入！"
    
    # 获取查找字符串
    while True:
        search_str = raw_input("请输入需要查找的字符串：").strip()
        if not search_str:
            print "❌ 查找字符串不能为空！"
            continue
        break
    
    return file_path, search_str

def main():
    """主函数：完全保留原有逻辑，仅增加交互模式支持"""
    reload(sys)
    sys.setdefaultencoding("utf-8")
    
    # 打印清理配置提示（原有逻辑）
    if CLEANUP_AFTER_IMPORT:
        print "⚠️ 清理配置：导入SQL后将自动删除解压目录(%s)及.sql临时文件" % DECOMPRESS_DIR
    else:
        print "ℹ️ 清理配置：导入SQL后保留所有临时文件"
    
    # 核心逻辑：优先保留原有命令行参数模式
    if len(sys.argv) != 3:
        # 参数数量不对时，先尝试交互模式，而不是直接退出
        try:
            file_name, search_str = get_input_interactive()
        except KeyboardInterrupt:
            print "\n\n⚠️ 用户中断操作，程序退出！"
            sys.exit(0)
        except Exception as e:
            print "\n❌ 交互模式错误：%s" % str(e)
            print_usage()
    else:
        # 完全保留原有参数解析逻辑
        file_name = sys.argv[1]
        search_str = sys.argv[2]
    
    # 执行核心处理逻辑（完全不变）
    process_single_file(file_name, search_str)

if __name__ == "__main__":
    main()
