CREATE TABLE IF NOT EXISTS registros(
    id INTEGER PRIMARY KEY,
    cod_localidad INTEGER,
    id_provincia INTEGER,
    id_departamento INTEGER,
    categoria CHARACTER VARYING,
    provincia CHARACTER VARYING,
    localidad CHARACTER VARYING,
    nombre CHARACTER VARYING,
    domicilio CHARACTER VARYING,
    "codigo postal" CHARACTER VARYING,
    "numero de telefono" CHARACTER VARYING,
    mail CHARACTER VARYING,
    web CHARACTER VARYING,
    fecha_carga DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS salas_provincias(
    id INTEGER PRIMARY KEY,
    provincia CHARACTER VARYING,
    pantallas INTEGER,
    butacas INTEGER,
    "espacios_INCAA" INTEGER,
    fecha_carga DATE NOT NULL
);