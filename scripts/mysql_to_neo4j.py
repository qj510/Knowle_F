from neo4j import GraphDatabase
import pymysql
import logging
import re
from config import DB_CONFIG, NEO4J_CONFIG  # 从配置文件中加载数据库配置

# 定义不同类型的实体对应的标签
ENTITY_LABELS = {
    '事件': 'Event',
    '人物': 'Person',
    '组织与联盟': 'Organization',
    '机构': 'Institution',
    '设施': 'Facility',
    '设备与工具': 'Equipment',
    '资源与物资': 'Resource',
    '商品': 'Commodity',
    '武器': 'Weapon',
    '协议与条约': 'Agreement',
    '国家': 'Country'
}

def sanitize_key(key):
    """Sanitize attribute keys by removing or replacing problematic characters."""
    sanitized = re.sub(r'[^\w]', '_', key)
    return sanitized

class Neo4jHandler:
    def __init__(self, config):
        self.driver = GraphDatabase.driver(config['uri'], auth=(config['user'], config['password']))

    def close(self):
        self.driver.close()

    def create_entity(self, entity_id, entity_type, entity_name, attributes):
        with self.driver.session() as session:
            session.execute_write(self._create_entity, entity_id, entity_type, entity_name, attributes)

    @staticmethod
    def _create_entity(tx, entity_id, entity_type, entity_name, attributes):
        label = ENTITY_LABELS.get(entity_type, 'Unknown')
        sanitized_attributes = {sanitize_key(k): v for k, v in attributes.items()}
        set_attributes = ', '.join([f"e.`{key}` = ${sanitize_key(key)}" for key in sanitized_attributes.keys()])

        if set_attributes:
            query = (
                f"MERGE (e:{label} {{id: $entity_id}}) "
                f"SET e.type = $entity_type, e.name = $entity_name, {set_attributes}"
            )
            tx.run(query, entity_id=entity_id, entity_type=entity_type, entity_name=entity_name, **sanitized_attributes)
        else:
            query = (
                f"MERGE (e:{label} {{id: $entity_id}}) "
                f"SET e.type = $entity_type, e.name = $entity_name"
            )
            tx.run(query, entity_id=entity_id, entity_type=entity_type, entity_name=entity_name)

        logging.info(f"Inserted Entity: {entity_id}, Type: {entity_type}, Name: {entity_name}, Label: {label}, Attributes: {sanitized_attributes}")

    def create_relationship(self, source_id, relation, target_id):
        with self.driver.session() as session:
            session.execute_write(self._create_relationship, source_id, relation, target_id)

    @staticmethod
    def _create_relationship(tx, source_id, relation, target_id):
        # 使用实际的关系类型代替 'RELATION'
        query = (
            f"MATCH (a {{id: $source_id}}), (b {{id: $target_id}}) "
            f"MERGE (a)-[r:`{relation}`]->(b)"
        )
        tx.run(query, source_id=source_id, relation=relation, target_id=target_id)
        logging.info(f"Inserted Relationship: {source_id} -[{relation}]-> {target_id}")

# 从 MySQL 中读取数据
def fetch_mysql_data():
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("SELECT entity_id, entity_type, entity_name FROM Entities")
        entities = cursor.fetchall()

        cursor.execute("SELECT entity_id, attribute_name, attribute_value FROM EntityAttributes")
        attributes_data = cursor.fetchall()

        attributes_dict = {}
        for entity_id, attr_name, attr_value in attributes_data:
            if entity_id not in attributes_dict:
                attributes_dict[entity_id] = {}
            attributes_dict[entity_id][attr_name] = attr_value

        cursor.execute("SELECT source_id, relation, target_id FROM Relationships")
        relationships = cursor.fetchall()

        cursor.close()
        conn.close()

        return entities, attributes_dict, relationships

    except pymysql.MySQLError as e:
        logging.error(f"MySQL error: {e}")
        return [], {}, []

def import_data_to_neo4j(entities, attributes_dict, relationships):
    neo4j_handler = Neo4jHandler(NEO4J_CONFIG)

    try:
        for entity_id, entity_type, entity_name in entities:
            attributes = attributes_dict.get(entity_id, {})
            neo4j_handler.create_entity(entity_id, entity_type, entity_name, attributes)

        for source_id, relation, target_id in relationships:
            neo4j_handler.create_relationship(source_id, relation, target_id)

        logging.info("Data import to Neo4j completed successfully.")

    except Exception as e:
        logging.error(f"Neo4j error: {e}")

    finally:
        neo4j_handler.close()

def main():
    entities, attributes_dict, relationships = fetch_mysql_data()

    if entities and relationships:
        import_data_to_neo4j(entities, attributes_dict, relationships)
    else:
        logging.warning("No data to import.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
