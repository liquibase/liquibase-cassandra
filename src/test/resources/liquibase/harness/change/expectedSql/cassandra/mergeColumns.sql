CREATE TABLE betterbotz.full_name_table (id INT, first_name VARCHAR, last_name VARCHAR, PRIMARY KEY (id))
INSERT INTO betterbotz.full_name_table (id, first_name, last_name) VALUES ('1', 'John', 'Doe')
INSERT INTO betterbotz.full_name_table (id, first_name, last_name) VALUES ('2', 'Jane', 'Doe')
ALTER TABLE betterbotz.full_name_table ADD full_name VARCHAR
UPDATE betterbotz.full_name_table SET full_name = first_name || ' ' || last_name
ALTER TABLE betterbotz.full_name_table DROP first_name
ALTER TABLE betterbotz.full_name_table DROP last_name