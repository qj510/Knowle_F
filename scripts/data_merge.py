import pymysql
from pymysql import MySQLError as Error
import logging
from config import DB_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def merge_duplicate_entities():
    conn = pymysql.connect(
        host=DB_CONFIG['host'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    cursor = conn.cursor()

    try:
        # 查找具有相同名称的实体
        cursor.execute("""
        SELECT entity_name, GROUP_CONCAT(entity_id) AS entity_ids
        FROM Entities 
        GROUP BY entity_name 
        HAVING COUNT(*) > 1
        """)
        duplicate_entities = cursor.fetchall()

        for entity in duplicate_entities:
            entity_name = entity['entity_name']
            entity_ids = entity['entity_ids']
            entity_id_list = entity_ids.split(',')
            primary_entity_id = entity_id_list[0]  # 将第一个实体作为主实体

            for duplicate_entity_id in entity_id_list[1:]:
                # 合并属性
                cursor.execute("""
                SELECT attribute_name, attribute_value 
                FROM EntityAttributes 
                WHERE entity_id = %s
                """, (duplicate_entity_id,))

                for attr in cursor.fetchall():
                    attr_name = attr['attribute_name']
                    attr_value = attr['attribute_value']
                    try:
                        cursor.execute("""
                        INSERT INTO EntityAttributes (entity_id, attribute_name, attribute_value) 
                        VALUES (%s, %s, %s)
                        """, (primary_entity_id, attr_name, attr_value))
                    except pymysql.IntegrityError:
                        # 如果属性已经存在，则跳过
                        logging.info(f"Attribute {attr_name} already exists for entity {primary_entity_id}, skipping.")

                # 合并关系
                cursor.execute("""
                UPDATE Relationships 
                SET source_id = %s 
                WHERE source_id = %s
                """, (primary_entity_id, duplicate_entity_id))

                cursor.execute("""
                UPDATE Relationships 
                SET target_id = %s 
                WHERE target_id = %s
                """, (primary_entity_id, duplicate_entity_id))

                # 删除重复的实体前先删除相关的属性
                cursor.execute("""
                DELETE FROM EntityAttributes 
                WHERE entity_id = %s
                """, (duplicate_entity_id,))

                # 删除重复的实体
                cursor.execute("""
                DELETE FROM Entities 
                WHERE entity_id = %s
                """, (duplicate_entity_id,))

            logging.info(f"Merged entities with name '{entity_name}' into entity {primary_entity_id}.")

        conn.commit()
        logging.info("Duplicate entity merging completed successfully.")

    except Error as e:
        logging.error(f"An error occurred during merging: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def main():
    merge_duplicate_entities()


if __name__ == "__main__":
    main()
