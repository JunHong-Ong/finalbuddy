// NOTE: The following script syntax is valid for database version 5.0 and above.

:param {
  // Define the file path root and the individual file names required for loading.
  // https://neo4j.com/docs/operations-manual/current/configuration/file-locations/
  file_path_root: 'file:///', // Change this to the folder your script can access the files at.
  file_0: 'concepts.csv',
  file_1: 'domains.csv',
  file_2: 'fields.csv',
  file_3: 'keywords.csv',
  file_4: 'subfields.csv',
  file_5: 'topics.csv',
  file_6: 'domains_has_field.csv',
  file_7: 'fields_has_subfield.csv',
  file_8: 'subfields_has_topic.csv',
  file_9: 'topics_has_keyword.csv'
};

// CONSTRAINT creation
// -------------------
//
// Create node constraints, ensuring no duplicates for the given node label and ID property exist in the database. This also ensures no duplicates are introduced in future.
//
CREATE CONSTRAINT `openalex_id_Concept_uniq` IF NOT EXISTS
FOR (n: `Concept`)
REQUIRE (n.`openalex_id`) IS UNIQUE;

CREATE CONSTRAINT `openalex_id_Domain_uniq` IF NOT EXISTS
FOR (n: `Domain`)
REQUIRE (n.`openalex_id`) IS UNIQUE;

CREATE CONSTRAINT `openalex_id_Field_uniq` IF NOT EXISTS
FOR (n: `Field`)
REQUIRE (n.`openalex_id`) IS UNIQUE;

CREATE CONSTRAINT `openalex_id_Keyword_uniq` IF NOT EXISTS
FOR (n: `Keyword`)
REQUIRE (n.`openalex_id`) IS UNIQUE;

CREATE CONSTRAINT `openalex_id_Subfield_uniq` IF NOT EXISTS
FOR (n: `Subfield`)
REQUIRE (n.`openalex_id`) IS UNIQUE;

CREATE CONSTRAINT `openalex_id_Topic_uniq` IF NOT EXISTS
FOR (n: `Topic`)
REQUIRE (n.`openalex_id`) IS UNIQUE;


:param {
  idsToSkip: []
};

// NODE load
// ---------
//
// Load nodes in batches, one node label at a time. Nodes will be created using a MERGE statement to ensure a node with the same label and ID property remains unique. Pre-existing nodes found by a MERGE statement will have their other properties set to the latest values encountered in a load file.
//
// NOTE: Any nodes with IDs in the 'idsToSkip' list parameter will not be loaded.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`ids.openalex` IN $idsToSkip AND NOT row.`ids.openalex` IS NULL
CALL (row) {
  MERGE (n: `Concept` { `openalex_id`: row.`ids.openalex` })
  SET n.`openalex_id` = row.`ids.openalex`
  SET n.`uuid` = randomUUID()
  SET n.`display_name` = row.`display_name`
  SET n.`level` = toInteger(row.`level`)
  SET n.`description` = row.`description`
  SET n.`wikidata_id` = row.`ids.wikidata`
  SET n.`wikipedia_id` = row.`ids.wikipedia`
  SET n.`umls_aui_id` = row.`ids.umls_aui`
  SET n.`umls_cui_id` = row.`ids.umls_cui`
  SET n.`mag_id` = row.`ids.mag`
  SET n.`works_count` = toInteger(trim(row.`works_count`))
  SET n.`cited_by_count` = toInteger(trim(row.`cited_by_count`))
  SET n.`created_at` = datetime()
  SET n.`updated_at` = datetime()
} IN TRANSACTIONS OF 10000 ROWS;


LOAD CSV WITH HEADERS FROM ($file_path_root + $file_1) AS row
WITH row
WHERE NOT row.`ids.openalex` IN $idsToSkip AND NOT row.`ids.openalex` IS NULL
CALL (row) {
  MERGE (n: `Domain` { `openalex_id`: row.`ids.openalex` })
  SET n.`openalex_id` = row.`ids.openalex`
  SET n.`uuid` = randomUUID()
  SET n.`display_name` = row.`display_name`
  SET n.`description` = row.`description`
  SET n.`wikidata_id` = row.`ids.wikidata`
  SET n.`wikipedia_id` = row.`ids.wikipedia`
  SET n.`works_count` = toInteger(trim(row.`works_count`))
  SET n.`cited_by_count` = toInteger(trim(row.`cited_by_count`))
  SET n.`created_at` = datetime()
  SET n.`updated_at` = datetime()
} IN TRANSACTIONS OF 10000 ROWS;


LOAD CSV WITH HEADERS FROM ($file_path_root + $file_2) AS row
WITH row
WHERE NOT row.`ids.openalex` IN $idsToSkip AND NOT row.`ids.openalex` IS NULL
CALL (row) {
  MERGE (n: `Field` { `openalex_id`: row.`ids.openalex` })
  SET n.`openalex_id` = row.`ids.openalex`
  SET n.`uuid` = randomUUID()
  SET n.`display_name` = row.`display_name`
  SET n.`description` = row.`description`
  SET n.`wikidata_id` = row.`ids.wikidata`
  SET n.`wikipedia_id` = row.`ids.wikipedia`
  SET n.`works_count` = toInteger(trim(row.`works_count`))
  SET n.`cited_by_count` = toInteger(trim(row.`cited_by_count`))
  SET n.`created_at` = datetime()
  SET n.`updated_at` = datetime()
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_3) AS row
WITH row
WHERE NOT row.`id` IN $idsToSkip AND NOT row.`id` IS NULL
CALL (row) {
  MERGE (n: `Keyword` { `openalex_id`: row.`id` })
  SET n.`openalex_id` = row.`id`
  SET n.`uuid` = randomUUID()
  SET n.`display_name` = row.`display_name`
  SET n.`works_count` = toInteger(trim(row.`works_count`))
  SET n.`cited_by_count` = toInteger(trim(row.`cited_by_count`))
  SET n.`created_at` = datetime()
  SET n.`updated_at` = datetime()
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_4) AS row
WITH row
WHERE NOT row.`ids.openalex` IN $idsToSkip AND NOT row.`ids.openalex` IS NULL
CALL (row) {
  MERGE (n: `Subfield` { `openalex_id`: row.`ids.openalex` })
  SET n.`openalex_id` = row.`ids.openalex`
  SET n.`uuid` = randomUUID()
  SET n.`display_name` = row.`display_name`
  SET n.`description` = row.`description`
  SET n.`wikidata_id` = row.`ids.wikidata`
  SET n.`wikipedia_id` = row.`ids.wikipedia`
  SET n.`works_count` = toInteger(trim(row.`works_count`))
  SET n.`cited_by_count` = toInteger(trim(row.`cited_by_count`))
  SET n.`created_at` = datetime()
  SET n.`updated_at` = datetime()
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_5) AS row
WITH row
WHERE NOT row.`ids.openalex` IN $idsToSkip AND NOT row.`ids.openalex` IS NULL
CALL (row) {
  MERGE (n: `Topic` { `openalex_id`: row.`ids.openalex` })
  SET n.`openalex_id` = row.`ids.openalex`
  SET n.`uuid` = randomUUID()
  SET n.`display_name` = row.`display_name`
  SET n.`description` = row.`description`
  SET n.`wikipedia_id` = row.`ids.wikipedia`
  SET n.`works_count` = toInteger(trim(row.`works_count`))
  SET n.`cited_by_count` = toInteger(trim(row.`cited_by_count`))
  SET n.`created_at` = datetime()
  SET n.`updated_at` = datetime()
} IN TRANSACTIONS OF 10000 ROWS;


// RELATIONSHIP load
// -----------------
//
// Load relationships in batches, one relationship type at a time. Relationships are created using a MERGE statement, meaning only one relationship of a given type will ever be created between a pair of nodes.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_6) AS row
WITH row 
CALL (row) {
  MATCH (source: `Domain` { `openalex_id`: row.`source_id` })
  MATCH (target: `Field` { `openalex_id`: row.`target.id` })
  MERGE (source)-[r: `HAS_FIELD`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_7) AS row
WITH row 
CALL (row) {
  MATCH (source: `Field` { `openalex_id`: row.`source_id` })
  MATCH (target: `Subfield` { `openalex_id`: row.`target.id` })
  MERGE (source)-[r: `HAS_SUBFIELD`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_8) AS row
WITH row 
CALL (row) {
  MATCH (source: `Subfield` { `openalex_id`: row.`source_id` })
  MATCH (target: `Topic` { `openalex_id`: row.`target.id` })
  MERGE (source)-[r: `HAS_TOPIC`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_9) AS row
WITH row 
CALL (row) {
  MATCH (source: `Topic` { `openalex_id`: row.`source_id` })
  MATCH (target: `Keyword` { `display_name`: row.`target` })
  MERGE (source)<-[r: `DESCRIBES`]-(target)
} IN TRANSACTIONS OF 10000 ROWS;
