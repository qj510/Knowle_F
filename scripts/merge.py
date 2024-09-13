import pymysql
import json
import logging

# 数据库配置
from config import DB_CONFIG

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def fetch_entity_data(cursor, entity_id):
    """
    从数据库中获取实体的名称、属性和关系
    """
    # 获取实体名称和类型
    cursor.execute("SELECT entity_name, entity_type FROM Entities WHERE entity_id = %s", (entity_id,))
    entity = cursor.fetchone()

    if not entity:
        logging.error(f"Entity {entity_id} not found.")
        return None

    # 获取实体属性
    cursor.execute("SELECT attribute_name, attribute_value FROM EntityAttributes WHERE entity_id = %s", (entity_id,))
    attributes = {row['attribute_name']: row['attribute_value'] for row in cursor.fetchall()}

    # 获取实体关系
    cursor.execute("SELECT source_id, relation, target_id FROM Relationships WHERE source_id = %s OR target_id = %s",
                   (entity_id, entity_id))
    relationships = cursor.fetchall()

    return {
        "name": entity['entity_name'],
        "type": entity['entity_type'],
        "attributes": attributes,
        "relationships": relationships
    }


def delete_entity(cursor, conn, entity_id):
    """
    删除实体及其相关的属性和关系
    """
    try:
        # 删除与该实体相关的关系记录
        cursor.execute("DELETE FROM Relationships WHERE source_id = %s OR target_id = %s", (entity_id, entity_id))
        conn.commit()
        logging.info(f"Deleted relationships associated with {entity_id}")

        # 删除与该实体相关的属性记录
        cursor.execute("DELETE FROM EntityAttributes WHERE entity_id = %s", (entity_id,))
        conn.commit()
        logging.info(f"Deleted attributes associated with {entity_id}")

        # 删除实体本身
        cursor.execute("DELETE FROM Entities WHERE entity_id = %s", (entity_id,))
        conn.commit()
        logging.info(f"Deleted entity {entity_id}")
    except pymysql.MySQLError as e:
        logging.error(f"Error deleting entity {entity_id}: {e}")


def merge_entities(cursor, conn, entity1_id, entity2_id):
    """
    将相似的两个实体合并，将第二个实体的属性和关系合并到第一个实体，然后删除第二个实体
    """
    # 获取两个实体的信息
    entity1 = fetch_entity_data(cursor, entity1_id)
    entity2 = fetch_entity_data(cursor, entity2_id)

    if not entity1 or not entity2:
        return

    # 合并属性
    for attr_name, attr_value in entity2['attributes'].items():
        if attr_name not in entity1['attributes']:
            logging.info(f"Merging attribute {attr_name} from {entity2_id} to {entity1_id}")
            cursor.execute(
                """
                INSERT INTO EntityAttributes (entity_id, attribute_name, attribute_value)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE attribute_value = VALUES(attribute_value);
                """, (entity1_id, attr_name, attr_value)
            )

    # 合并关系
    for relationship in entity2['relationships']:
        source_id = relationship['source_id']
        target_id = relationship['target_id']
        relation = relationship['relation']

        # 如果 source_id 是 entity2_id，将其改为 entity1_id
        if source_id == entity2_id:
            source_id = entity1_id
        # 如果 target_id 是 entity2_id，将其改为 entity1_id
        if target_id == entity2_id:
            target_id = entity1_id

        logging.info(f"Merging relationship {source_id} - {relation} -> {target_id} from {entity2_id} to {entity1_id}")
        cursor.execute(
            """
            INSERT INTO Relationships (source_id, relation, target_id)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE source_id = VALUES(source_id), target_id = VALUES(target_id);
            """, (source_id, relation, target_id)
        )

    # 删除第二个实体及其相关的属性和关系
    delete_entity(cursor, conn, entity2_id)


def process_high_similarity_pairs(high_similarity_file_path):
    """
    处理高相似度的实体对，将相似度大于 0.95 的实体进行合并
    """
    with open(high_similarity_file_path, 'r', encoding='utf-8') as file:
        high_similarity_pairs = json.load(file)

    conn = None
    cursor = None
    try:
        # 连接数据库
        conn = pymysql.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        cursor = conn.cursor()

        # 遍历相似度高的实体对，进行合并
        for entity_type, pairs in high_similarity_pairs.items():
            # 检查 pairs 是否为空
            if not pairs:
                logging.info(f"No high similarity pairs found for entity type: {entity_type}")
                continue

            logging.info(f"Processing entity type: {entity_type}")

            for entity1_id, entity2_id in pairs.items():
                logging.info(f"Fetching data for entity: {entity1_id}")
                logging.info(f"Fetching data for entity: {entity2_id}")
                merge_entities(cursor, conn, entity1_id, entity2_id)

    except pymysql.MySQLError as e:
        logging.error(f"Database error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def main():
    high_similarity_file_path = 'high_similarity_pairs.json'  # 确保文件路径正确
    process_high_similarity_pairs(high_similarity_file_path)


if __name__ == "__main__":
    main()
