CREATE DATABASE IF NOT EXISTS dbo;
USE dbo;

CREATE TABLE IF NOT EXISTS Usuarios (
    userId INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    rol ENUM('admin','user') DEFAULT 'user',
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS datos_importados (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(255),
    apellido VARCHAR(255),
    email VARCHAR(255),
    match_query TEXT,
    match_result TEXT,
    score DECIMAL(5,2),
    match_result_values TEXT,
    destTable VARCHAR(255),
    sourceTable VARCHAR(255),
    fecha_insercion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DELIMITER $$

CREATE PROCEDURE InsertMatchedRecord (
    IN p_nombre VARCHAR(255),
    IN p_apellido VARCHAR(255),
    IN p_email VARCHAR(255),
    IN p_match_query TEXT,
    IN p_match_result TEXT,
    IN p_score DECIMAL(5,2),
    IN p_match_result_values TEXT,
    IN p_destTable VARCHAR(255),
    IN p_sourceTable VARCHAR(255)
)
BEGIN
    INSERT INTO datos_importados (
        nombre, apellido, email, match_query, match_result,
        score, match_result_values, destTable, sourceTable
    )
    VALUES (
        p_nombre, p_apellido, p_email, p_match_query, p_match_result,
        p_score, p_match_result_values, p_destTable, p_sourceTable
    );
END$$

DELIMITER ;
