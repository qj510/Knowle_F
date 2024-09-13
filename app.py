# app.py
from flask import Flask, render_template, request, redirect, url_for, send_from_directory, jsonify
from flask_socketio import SocketIO, emit
import os
import subprocess
import threading
import time

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = './upload'  # 上传文件保存的文件夹
socketio = SocketIO(app, cors_allowed_origins="*")

# 确保上传文件的目录存在
if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])


# 首页展示
@app.route('/')
def index():
    return render_template('index.html')


# 上传文件处理
@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return redirect(url_for('index'))

    file = request.files['file']
    if file.filename == '':
        return redirect(url_for('index'))

    if file:
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        file.save(file_path)

        # 开启线程运行处理脚本，避免阻塞 Flask 主线程
        thread = threading.Thread(target=process_file, args=(file_path,))
        thread.start()

        return redirect(url_for('index'))


# 处理上传的文件，执行知识抽取和入库脚本
def process_file(file_path):
    """
    Process the uploaded file by running the extraction, ingestion, and fusion scripts sequentially.
    """
    try:
        log("Starting knowledge extraction...")
        # 运行知识抽取脚本
        run_script(['python', './scripts/extract.py', '--file_path', file_path])

        log("Starting knowledge import to database...")
        # 运行知识入库脚本
        run_script(['python', './scripts/data_ingestion.py'])

        log("Starting knowledge fusion...")
        # 运行知识融合脚本
        run_script(['python', './scripts/data_merge.py'])
        run_script(['python', './scripts/knowfusion.py'])
        run_script(['python', './scripts/merge.py'])

        # 转入图数据库中
        log("Transfer to a graph database...")
        run_script(['python', './scripts/mysql_to_neo4j.py'])

        log("All processes completed successfully.")
    except Exception as e:
        log(f"Error during processing: {str(e)}")


def run_script(command):
    """
    Runs a script with the given command and waits for it to complete. Logs output in real-time.
    """
    try:
        # 使用 subprocess.run 以阻塞方式执行脚本，确保每个脚本顺序执行
        result = subprocess.run(command, capture_output=True, text=True, check=True, encoding='utf-8')

        # 实时输出执行结果
        if result.stdout:
            for line in result.stdout.splitlines():
                log(line.strip())

        # 检查是否有错误输出
        if result.stderr:
            log(f"Error output: {result.stderr}")

    except subprocess.CalledProcessError as e:
        # 捕获脚本执行错误，输出错误信息并中止后续脚本执行
        log(f"Error executing script {command}: {e.stderr}")
        raise e  # 重新抛出异常以终止后续脚本执行
# 向前端发送日志
def log(message):
    """
    发送日志消息到前端
    """
    print(message)  # 服务器端打印
    socketio.emit('log', {'data': message}, to='/')  # 将消息发送到所有客户端


# SSE 日志输出
@app.route('/logs')
def stream_logs():
    return redirect(url_for('index'))


# 运行 Flask 应用
if __name__ == "__main__":
    socketio.run(app, debug=True, allow_unsafe_werkzeug=True, port=5001)


