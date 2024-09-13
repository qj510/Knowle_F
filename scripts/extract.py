import requests
import json
import chardet
import os
import chardet

# 自动检测文件编码并读取文本文件
import chardet
import os

def read_text_file(file_path):
    # 确保文件路径和文件名使用 utf-8 处理
    try:
        # 如果路径包含非 ASCII 字符，可以先规范化为 utf-8
        file_path = os.fsdecode(file_path)
        print(f"Normalized file path: {file_path}")
    except UnicodeDecodeError as path_error:
        print(f"Error decoding file path with utf-8: {path_error}")

    # 以二进制模式读取文件并检测编码
    with open(file_path, 'rb') as file:
        raw_data = file.read()
        detected_encoding = chardet.detect(raw_data)['encoding']
        print(f"Detected Encoding: {detected_encoding}")

    # 使用检测到的编码读取文件内容，处理无法解码的字符
    try:
        # 尝试使用检测到的编码读取文件
        with open(file_path, 'r', encoding=detected_encoding, errors='ignore') as file:
            return file.read()
    except (UnicodeDecodeError, TypeError) as e:
        print(f"Error reading file with detected encoding {detected_encoding}: {e}")

        # 如果检测到的编码无效或读取失败，尝试使用其他常见编码
        fallback_encodings = ['utf-8', 'utf-8-sig', 'gb18030']
        for encoding in fallback_encodings:
            try:
                print(f"Trying fallback encoding: {encoding}")
                with open(file_path, 'r', encoding=encoding, errors='ignore') as file:
                    return file.read()
            except UnicodeDecodeError as fallback_error:
                print(f"Error with fallback encoding {encoding}: {fallback_error}")

    # 如果所有编码尝试均失败，抛出异常
    raise ValueError("Failed to read the file with any attempted encoding.")


# 获取最近上传的文件名
def get_latest_uploaded_file(directory):
    files = [os.path.join(directory, f) for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    if not files:
        raise FileNotFoundError("No files found in the upload directory.")
    latest_file = max(files, key=os.path.getmtime)  # 根据修改时间找到最新的文件
    return latest_file
def split_text(text, max_length=5000):
    """
    将文本按固定字数分割，每段最多 max_length 个字符。

    :param text: 待分割的文本字符串
    :param max_length: 每段的最大字符数，默认为 1000
    :return: 分割后的文本段列表
    """
    segments = [text[i:i + max_length] for i in range(0, len(text), max_length)]
    return segments

# 发送请求给大模型并返回结果
def get_extracted_info(prompt, model_url, goal, rule):
    full_prompt = f"##Goal{goal}\n##Rules{rule}\n##Input{prompt.replace(chr(10), ' ')}"
    data = {"prompt": full_prompt}
    json_data = json.dumps(data, ensure_ascii=False)
    wrapped_data = {
        "data": json_data
    }
    print("Request Data:", wrapped_data)  # 调试信息
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    response = requests.post(model_url, data=wrapped_data, headers=headers)
    print("Response Status Code:", response.status_code)
    detected_encoding = chardet.detect(response.content)
    print("Detected Encoding:", detected_encoding)
    encoding = detected_encoding['encoding'] if detected_encoding['encoding'] else 'utf-8'
    response_text = response.content.decode(encoding)
    print("Response Content:", response_text)  # 调试信息

    if response.status_code != 200:
        raise ValueError(f"Error from model: {response.status_code} {response_text}")

    return response_text

# 提取信息并保存为JSON格式，同时记录进度
def extract_and_save_info(text_chunks, model_url, output_file, goal, rule, progress_file):
    extracted_info = []
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as file:
            extracted_info = json.load(file)

    # 读取进度文件
    if os.path.exists(progress_file):
        with open(progress_file, 'r', encoding='utf-8') as file:
            processed_ids = set(json.load(file))
    else:
        processed_ids = set()

    for index, chunk in enumerate(text_chunks):
        if index in processed_ids:
            continue

        result = get_extracted_info(chunk, model_url, goal, rule)
        try:
            result_json = json.loads(result)
            extracted_info.append(result_json)
            processed_ids.add(index)

            # 保存提取的信息和进度
            with open(output_file, 'w', encoding='utf-8') as file:
                json.dump(extracted_info, file, ensure_ascii=False, indent=4)
            with open(progress_file, 'w', encoding='utf-8') as file:
                json.dump(list(processed_ids), file, ensure_ascii=False, indent=4)
        except json.JSONDecodeError:
            print(f"Error decoding JSON response: {result}")


# 主函数
def main():
    # 获取上传文件目录中的最新文件
    upload_directory = './upload'  # 指定上传文件的目录
    file_path = get_latest_uploaded_file(upload_directory)  # 获取最新上传的文件路径
    print(f"Processing file: {file_path}")
    model_url = 'http://101.251.216.48:80/qwen2-110B-stream'
    output_file = 'extracted_info_01.json'
    progress_file = 'progress_01.json'
    goal = """
    ##Goal
    现在你是一名情报分析师，你的任务是从输入文档中提取所有实体及其属性，并描述实体之间的关系，提取的实体类型包括：国家（country）、人物（character）、组织与联盟（tissue）、机构（institution）、设施（facility）、设备与工具(Equipment and tools)、资源与物资(Resources and materials)、商品(commodity)、武器（weapon）、协议与条约（Agreements and treaties）、事件（incident）。返回json数组结构。
    """
    rule = """
    ##Rule
    要求，你所返回的json数组外不要加三引号概括;
    要求，对于文档中的所有实体必须要求有确切的名称，不能确定名称以及没有名称的实体不提取;
    要求，没有的实体不提取，不要无中生有;
    要求，文档中包含多个实体，必须从文档中提取全部的实体及其属性，不能遗漏，要求提取全面且准确，属性类型为空的属性则不提取;
    要求，提取实体的同时必须保留提取该实体的关键句子（key sentence）,即从哪句话提取到的该实体,并保存该句子;
    要求，对实体的属性提取必须全面且具体;
    要求，根据实体个数可以创建多个json数组;
    要求，人物（character），只提取有具体名称的人物，如“姚明”。没有具体名称的人物不提取(如“赵同学”、“张老师”）;
    要求，人物（character），注意！职业名称不属于人物，(如“飞行员”、“连长”这一类属于人物的职称，不属于人物（character）)，属于人物的属性;
    要求，人物（character），严格区分职业名称和人物名称，没有具体姓名的人物不提取;
    要求，人物（character），根据全文理解人物昵称，昵称属于人物属性，如人物“宋江”的称号为“及时雨”;
    要求，人物（character），根据全文语义理解并提取“人物”所属的国家;
    要求，人物（character），根据全文语义理解并提取“人物”所属的机构或组织;
    要求，组织与联盟（tissue），组织的二级分类有：政治组织、经济组织、文化组织、军事组织、宗教组织;
    要求，组织与联盟（tissue），根据全文语义理解组织与联盟属于那种类型，并记录为属性;
    要求，机构（institution），机构的二级分类有：政治机构、经济机构、文化机构、军事机构、宗教机构;
    要求，机构（institution），根据全文语义了解机构类型，并记录为属性;
    要求，设施（facility），设施是为实现特定用途或支持日常运作而设立的各类建筑、设备和基础系统，包括交通、电力、通信等关键服务的硬件结构;
    要求，武器（weapon），只提取具有明确属性及有具体型号的武器，如（“052D导弹驱逐舰”），对于没有任何属性的武器或统称的武器（如“步枪”、“导弹”）不提取；
    要求，武器（weapon），武器型号、名称、属性提取必须全面具体;
    要求，武器（weapon），武器的二级分类有：轻武器（如手枪、步枪、冲锋枪）、重武器（如机枪、火箭筒、榴弹发射器）、火炮（如坦克炮、反坦克炮、迫击炮）、导弹（如地对空导弹、反舰导弹、巡航导弹）、核武器（如核弹头、核导弹）、战斗车辆（如坦克、装甲车）、航空器（如战斗机、轰炸机、无人机）、海军武器（如战舰、潜艇、鱼雷）、太空武器（如反卫星导弹、轨道轰炸武器）。根据全文语义理解武器种类，并记录为属性;
    要求，设备与工具(Equipment and tools)，根据全文语义了解设备与工具的作用。
    要求，设备与工具(Equipment and tools)，各种机械设备、工具、仪器等，涵盖了工业设备、家用电器、手工工具等都属于此类;
    要求，资源与物资(Resources and materials)，项目管理、物流、供应链等场景，描述需要管理、运输或存储的物品和材料以及用于自然资源、能源、原材料等，适合描述生产制造中使用的基础资源或可利用的自然资源都属于此类;
    要求，商品(commodity)，主要用于商业和零售领域，涵盖所有用于销售的实体产品都属于此类;
    要求，严格区分商品(commodity)与武器（weapon），即使有交易属性，武器依然归类于武器类;
    要求，协议与条约（Agreements and treaties），协议与条约的名称、签署发布的时间地点必须提取准确具体全面;
    要求，协议与条约（Agreements and treaties），根据全文语义了解协议与条约的作用;
    要求，国家（country），可以是事件实施者;
    要求，事件（incident），事件的发生时间（time）与地点（location）必须提取，且必须准确;
    要求，事件描述泛化（des_general）对提取的事件描述（description）字段进行泛化，用动宾短语概括事件，忽略其中主体、客体;
    要求，事件结果泛化（result_general）对提取的事件发生结果（result）字段进行泛化，用动宾短语概括事件，忽略其中主体、客体;
    要求，事件的参与者（participant）、目标（goal）、地点（location）属于事件的属性;
    要求，关系（relation），对实体与实体关系的描述要求尽可能简短;
    要求，只返回JSON数组，不返回其他任何额外信息;
    要求，数据只从输入文档input中提取，提取数据要求全面且准确无误，不能无中生有、胡编乱造;
    要求，你需要严格按照参考的格式输出，要求输出的json格式无误并且可以正确解析，json中不需要注释信息;
    
    
    
    ##example
    Json数据格式如下：
    {
    "entities":[
    {
    "id":"entity1",
    "type":"人物",
    "name":"张三",
    "key sentence":"张三是抗日的大英雄",
    "attributes":{
    "昵称":"大英雄",
    "职位":"将军",
    "年龄":45,
    "国籍":"中国"
    }
    },
    {
    "id":"entity2",
    "type":"军事单位",
    "name":"第47部队",
    "key sentence":"第47部队是驻扎在北京的军事单位",
    "attributes":{
    "规模":"5000人",
    "驻地":"北京"
    }
    },
    {
    "id":"entity3",
    "type":"武器",
    "name":"AK-47步枪",
    "key sentence":"AK-47步枪是一把威力十足的步枪",
    "attributes":{
    "二级类别":"轻武器",
    "生产年份":1949,
    "射速":600RPM
    }
    },
    {
    "id":"entity4",
    "type":"事件",
    "name":"华莱士战役",
    "key sentence":"2022年7月15日，发生了华莱士战役",
    "attributes":{
    "时间":"2022-07-15",
    "地点":"某地",
    "结果":"胜利"
    }
    }
    ],
    "relationships":[
    {
    "source":"entity1",
    "target":"entity2",
    "relation":"指挥"
    },
    {
    "source":"entity2",
    "target":"entity4",
    "relation":"参与"
    },
    {
    "source":"entity1",
    "target":"entity3",
    "relation":"使用"
    }
    ]
    
    },
    {
    "entities":[
    {
    "id":"entity1",
    "type":"人物",
    "name":"李四",
    "key sentence":"李四终于当上了公务员",
    "attributes":{
    "性格":"开朗",
    "职位":"公务员",
    "年龄":55,
    "国籍":"日本"
    }
    },
    {
    "id":"entity2",
    "type":"组织与联盟",
    "name":"复仇者联盟",
    "key sentence":"李四创建了复仇者联盟",
    "attributes":{
    "规模":"8",
    "驻地":"东京"
    }
    },
    {
    "id":"entity3",
    "type":"武器"
    "name":"5901舰",
    "key sentence":"复仇者联盟驾驶着5901舰",
    "attributes":{
    "二级类别":"海军武器",
    "重量": "12000吨",
    "生产年份":2003
    }
    },
    {
    "id":"entity4",
    "type":"事件",
    "name":"终局之战",
    "key sentence":"2022年7月15日发生了终局之战",
    "attributes":{
    "时间":"2022-07-15",
    "地点":"某地",
    "结果":"胜利"
    }
    }
    ],
    "relationships":[
    {
    "source":"entity1",
    "target":"entity2",
    "relation":"创建"
    },
    {
    "source":"entity2",
    "target":"entity4",
    "relation":"参与"
    },
    {
    "source":"entity2",
    "target":"entity3",
    "relation":"使用"
    }
    ]
    },
    ......
    
    
    ##input

    """
    text = read_text_file(file_path)
    text_chunks = split_text(text)
    extract_and_save_info(text_chunks, model_url, output_file, goal, rule, progress_file)

if __name__ == "__main__":
    main()

