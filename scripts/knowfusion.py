import pymysql
import random
import json
from modelscope.pipelines import pipeline
from modelscope.utils.logger import get_logger
import torch
from itertools import combinations


def load_config(config_path='config.json'):
    """
    从配置文件加载数据库和模型相关的配置。
    """
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    return config


def connect_to_database(config):
    """
    连接到数据库。
    """
    connection = pymysql.connect(
        host=config['db_host'],
        user=config['db_user'],
        password=config['db_password'],
        database=config['db_name']
    )
    return connection


def fetch_entity_names_with_ids(connection, table_name, entity_type):
    """
    获取指定 entity_type 下的所有 entity_name 及其对应的 entity_id。
    """
    cursor = connection.cursor()
    cursor.execute(f"SELECT entity_id, entity_name FROM {table_name} WHERE entity_type = %s", (entity_type,))
    return cursor.fetchall()


def main():
    # 加载配置文件
    config = load_config()

    # 连接到数据库
    connection = connect_to_database(config)

    # 检查 GPU 可用性并设置设备
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Using device: {device}")

    # 初始化模型
    MODEL_PATH = config['local_model_path']
    # 传递 device 为字符串类型
    nlp_model = pipeline(task='sentence-similarity', model=MODEL_PATH, device=device)
    logger = get_logger()

    # 固定的实体类型列表
    fixed_entity_types = ["事件", "人物", "武器", "资源与物资", "组织与联盟", "设施"]

    # 存储所有类型的高相似度实体对
    all_high_similarity_pairs = {}

    # 遍历所有实体类型
    for entity_type in fixed_entity_types:
        print(f"正在处理实体类型: {entity_type}")

        # 获取该 entity_type 下的所有 entity_name 及其对应的 entity_id
        entity_data = fetch_entity_names_with_ids(connection, config['table_name'], entity_type)

        # 以相似度大于 0.7 的词对存储 entity_id 的键值对
        high_similarity_pairs = {}

        # 对所有的 entity_name 两两比较
        for (id1, name1), (id2, name2) in combinations(entity_data, 2):
            # 计算两个词汇的相似度
            similarity_result = nlp_model({'text': name1, 'text_target': name2})
            similarity_scores = similarity_result['scores']

            # 获取相似度分数
            similarity_score = similarity_scores[1]

            # 如果相似度大于 0.7，将两个词的 entity_id 以键值对形式存储
            if similarity_score > 0.7:
                high_similarity_pairs[id1] = id2
                print(f"'{name1}' 和 '{name2}' 的相似度为: {similarity_score:.4f}")
            else:
                print(f"'{name1}' 和 '{name2}' 的相似度为: {similarity_score:.4f}")

        # 将当前类型的高相似度词对加入到总的字典中
        all_high_similarity_pairs[entity_type] = high_similarity_pairs

    # 将所有类型的高相似度词对以 JSON 格式保存
    with open('high_similarity_pairs.json', 'w', encoding='utf-8') as f:
        json.dump(all_high_similarity_pairs, f, ensure_ascii=False, indent=4)

    print(f"所有高相似度词对已保存到 'high_similarity_pairs.json' 文件中。")

    # 关闭数据库连接
    connection.close()


if __name__ == "__main__":
    main()
