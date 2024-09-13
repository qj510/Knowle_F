import pymysql
from pymysql import MySQLError as Error
import logging
import uuid
import json
import os

# 数据库配置
from config import DB_CONFIG

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def create_tables(cursor):
    # 创建表结构
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Entities (
        entity_id VARCHAR(255) PRIMARY KEY,
        entity_type VARCHAR(255),
        entity_name VARCHAR(255)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS EntityAttributes (
        id INT AUTO_INCREMENT PRIMARY KEY,
        entity_id VARCHAR(255),
        attribute_name VARCHAR(255),
        attribute_value VARCHAR(255),
        FOREIGN KEY (entity_id) REFERENCES Entities(entity_id),
        UNIQUE(entity_id, attribute_name)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Relationships (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source_id VARCHAR(255),
        relation VARCHAR(255),
        target_id VARCHAR(255),
        FOREIGN KEY (source_id) REFERENCES Entities(entity_id),
        FOREIGN KEY (target_id) REFERENCES Entities(entity_id)
    );
    """)


def read_file(file_path):
    # 尝试使用 utf-8 编码读取文件，若失败则报告错误
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except UnicodeDecodeError as e:
        logging.error(f"Error reading file {file_path} with utf-8 encoding: {e}")
        raise


def load_and_parse_data(file_path):
    error_data = []
    valid_data = []

    try:
        # 读取文件内容
        raw_content = read_file(file_path)
        # 使用标准 JSON 解析文件内容
        data_blocks = json.loads(raw_content)

        # 确保数据为列表格式
        if not isinstance(data_blocks, list):
            raise ValueError("Top-level JSON element must be a list of blocks.")

        for block_index, block in enumerate(data_blocks):
            # 检查每个 block 是否为列表
            if not isinstance(block, list):
                logging.error(f"Block {block_index} is not a list. Skipping...")
                error_data.append(f"Block {block_index}: {json.dumps(block)} (Not a list)")
                continue
            valid_data.append(block)
    except (ValueError, json.JSONDecodeError) as e:
        logging.error(f"Failed to parse JSON file: {e}")
        error_data.append(f"Error parsing JSON file: {e}")

    return valid_data, error_data


def save_error_data(error_data, error_file_path):
    # 保存解析失败的错误数据
    if not error_data:
        logging.info("No errors found in JSON data.")
        return

    with open(error_file_path, 'w', encoding='utf-8') as f:
        for item in error_data:
            f.write(item + "\n")
    logging.info(f"Error data saved to {error_file_path}")


def insert_data(cursor, conn, data_blocks):
    id_mapping = {}  # 创建原始entity_id到UUID的映射
    for block in data_blocks:
        for item in block:
            try:
                insert_entities(cursor, item, id_mapping)
                insert_relationships(cursor, item, id_mapping)
                conn.commit()  # 使用数据库连接对象进行提交
            except Exception as e:
                logging.error(f"Error processing item: {item}. Error: {e}")
                conn.rollback()  # 使用数据库连接对象进行回滚


def insert_entities(cursor, item, id_mapping):
    entities = item.get("entities", [])
    for entity in entities:
        original_entity_id = entity['id']
        entity_id = str(uuid.uuid4())  # 生成UUID作为新的entity_id
        entity_type = entity['type']
        entity_name = entity['name']

        logging.info(f"Inserting entity: {entity_id}, {entity_type}, {entity_name}")
        cursor.execute("""
        INSERT INTO Entities (entity_id, entity_type, entity_name) 
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE entity_type = VALUES(entity_type), entity_name = VALUES(entity_name);
        """, (entity_id, entity_type, entity_name))

        # 保存原始entity_id到UUID的映射
        id_mapping[original_entity_id] = entity_id

        attributes = entity.get('attributes', {})
        for attr_name, attr_value in attributes.items():
            if attr_value is None:
                attr_value = 'NULL'
            elif isinstance(attr_value, list):
                attr_value = ','.join(map(str, attr_value))
            logging.info(f"Inserting attribute for {entity_id}: {attr_name} = {attr_value}")
            cursor.execute("""
            INSERT INTO EntityAttributes (entity_id, attribute_name, attribute_value) 
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE attribute_value = VALUES(attribute_value);
            """, (entity_id, attr_name, attr_value))


def insert_relationships(cursor, item, id_mapping):
    relationships = item.get("relationships", [])
    for relationship in relationships:
        source_id = id_mapping.get(relationship['source'])
        target_id = id_mapping.get(relationship['target'])
        relation = relationship['relation']

        if source_id and target_id:
            logging.info(f"Inserting relationship: {source_id} - {relation} -> {target_id}")
            cursor.execute("""
            INSERT INTO Relationships (source_id, relation, target_id) 
            VALUES (%s, %s, %s);
            """, (source_id, relation, target_id))
        else:
            logging.error(f"Relationship source or target not found: {relationship}")


def main():
    # 获取当前脚本的绝对路径
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # 获取项目根目录路径
    root_dir = os.path.dirname(script_dir)

    # 指定 JSON 文件路径
    file_path = os.path.join(root_dir, 'extracted_info_01.json')
    error_file_path = os.path.join(root_dir, 'error_data.json')  # 错误数据保存的路径

    valid_data, error_data = load_and_parse_data(file_path)

    if error_data:
        save_error_data(error_data, error_file_path)

    if valid_data:
        conn = None
        cursor = None
        try:
            conn = pymysql.connect(
                host=DB_CONFIG['host'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                database=DB_CONFIG['database'],
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            cursor = conn.cursor()
            create_tables(cursor)
            insert_data(cursor, conn, valid_data)  # 将连接对象传递给 insert_data
        except Error as e:
            logging.error(f"Database error: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    else:
        logging.warning("No valid data available to insert into the database.")


if __name__ == "__main__":
    main()
