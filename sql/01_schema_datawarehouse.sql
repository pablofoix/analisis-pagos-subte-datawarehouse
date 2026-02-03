IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'Subtes')
BEGIN
CREATE DATABASE Subtes;
END
GO
USE Subtes;
GO

-- ========================
-- DIMENSION TIEMPO
-- ========================
IF OBJECT_ID('Tiempo', 'U') IS NULL
BEGIN
CREATE TABLE Tiempo (
    id_tiempo INT IDENTITY(1,1) PRIMARY KEY,
    fecha DATE NOT NULL UNIQUE,
    dia INT,
    mes INT,
    trimestre INT,
    semestre INT,
    anio INT
);
END

-- ========================
-- DIMENSION TURNO
-- ========================
IF OBJECT_ID('Turno', 'U') IS NULL
BEGIN
CREATE TABLE Turno (
    id_turno INT IDENTITY(1,1) PRIMARY KEY,
    desde TIME NOT NULL,
    hasta TIME NOT NULL,
    UNIQUE (desde, hasta)
);
END

-- ========================
-- DIMENSION ESTACION
-- ========================
IF OBJECT_ID('Estacion', 'U') IS NULL
BEGIN
CREATE TABLE Estacion (
    id_estacion INT IDENTITY(1,1) PRIMARY KEY,
    linea_subte VARCHAR(10) NOT NULL,
    nombre_estacion VARCHAR(100) NOT NULL,
    latitud DECIMAL(9,6),
    longitud DECIMAL(9,6),
    UNIQUE (linea_subte, nombre_estacion)
);
END

-- ========================
-- FACT TABLE
-- ========================
IF OBJECT_ID('Registros', 'U') IS NULL
BEGIN
CREATE TABLE Registros (
    id_registro INT IDENTITY(1,1) PRIMARY KEY,
    id_tiempo INT NOT NULL,
    id_turno INT NOT NULL,
    id_estacion INT NOT NULL,
    pax_pagos INT NOT NULL,
    pax_pases_pagos INT NOT NULL,
    pax_franq INT NOT NULL,
    pax_total INT NOT NULL,

    CONSTRAINT FK_Tiempo FOREIGN KEY (id_tiempo) REFERENCES Tiempo(id_tiempo),
    CONSTRAINT FK_Turno FOREIGN KEY (id_turno) REFERENCES Turno(id_turno),
    CONSTRAINT FK_Estacion FOREIGN KEY (id_estacion) REFERENCES Estacion(id_estacion)
);
END