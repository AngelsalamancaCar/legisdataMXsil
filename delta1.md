1. columna anio_nacimiento cambiar nombre a y_nacimiento
2. eliminar las columnas tiene_suplente, tiene_telefono, tiene_ubicacion
3. Agregar 2 varibles binarias para reordenar legislativa clasificacion de experiencia legislativa. Si experiencia_legislativa = "Senador" se genera columna exp_senado = 1, si experiencia_legislativa = "Diputado Federal" se genera columna exp_dip_federal = 1, si experiencia_legislativa = "Diputado Local" se genera columna exp_dip_local=1. 
4. Eliminar columna "redes_sociales"
5. la columna comisiones que actualmente tiene el formato: [
    {
        "Comisión": "Especial sobre Migración (C. Diputados)",
        "Puesto": "Integrante",
        "Fecha Inicial": "28/04/2011",
        "Fecha Final": "31/08/2012",
        "Estatus": "Activo"
    },
    {
        "Comisión": "Especial para la Industria Manufacturera de Exportación. (C. Diputados)",
        "Puesto": "Integrante",
        "Fecha Inicial": "20/04/2010",
        "Fecha Final": "31/08/2012",
        "Estatus": "Activo"
    },
    {
        "Comisión": "Población, Fronteras y Asuntos Migratorios (C. Diputados)",
        "Puesto": "Secretario",
        "Fecha Inicial": "31/08/2009",
        "Fecha Final": "31/08/2012",
        "Estatus": "Activo"
    },
    {
        "Comisión": "Fortalecimiento al Federalismo (C. Diputados)",
        "Puesto": "Integrante",
        "Fecha Inicial": "31/08/2009",
        "Fecha Final": "31/08/2012",
        "Estatus": "Activo"
    }
]
Esta se debe transformar a una version desagregada en horizontal con una agrupacion de XXX columnas con una primera columna comision_# donde se inserta el value de la key "Comisión", una segunda columna puesto_comision_# donde se inserta el value del key "Puesto".

6. Eliminar la columna licencias_reincorporaciones
7. Modificar la columna trayectoria_administrativa 
[
    {
        "Del año": "2008",
        "Al año": "2009",
        "Experiencia": "Regidora en Nuevo Laredo, Tamaulipas."
    },
    {
        "Del año": "2005",
        "Al año": "2007",
        "Experiencia": "Subdirectora de desarrollo social en Nuevo Laredo, Tamaulipas."
    },
    {
        "Del año": "2002",
        "Al año": "2004",
        "Experiencia": "Regidor suplente en Nuevo Laredo, Tamaulipas."
    },
    {
        "Del año": "2002",
        "Al año": "2004",
        "Experiencia": "Coordinadora municipal de atención a los jóvenes en Nuevo Laredo, Tamaulipas."
    },
    {
        "Del año": "1998",
        "Al año": "1998",
        "Experiencia": "Coordinadora del programa municipal Manos Juveniles en Nuevo Laredo, Tamaulipas."
    },
    {
        "Del año": "1996",
        "Al año": "1997",
        "Experiencia": "Directora de Atención a la Juventud en IMSS de Nuevo Laredo, Tamaulipas."
    },
    {
        "Del año": "TRAYECTORIA POLÍTICA",
        "Al año": "",
        "Experiencia": ""
    },
    {
        "Del año": "2009",
        "Al año": "",
        "Experiencia": "Presidente del Frente Juvenil Revolucionario, en Nuevo Laredo, Tamaulipas."
    },
    {
        "Del año": "2006",
        "Al año": "2006",
        "Experiencia": "Enlace juvenil de la campaña interna por la dirigencia nacional del PRI."
    },
    {
        "Del año": "2005",
        "Al año": "",
        "Experiencia": "Integrante de la Fundación Colosio A.C. filial Nuevo Laredo."
    },
    {
        "Del año": "2003",
        "Al año": "2003",
        "Experiencia": "Candidata a diputada federal en la LIX Legislatura del Congreso de la Unión."
    },
    {
        "Del año": "2003",
        "Al año": "2009",
        "Experiencia": "Consejera nacional del PRI."
    },
    {
        "Del año": "2002",
        "Al año": "2004",
        "Experiencia": "Coordinadora juvenil del movimiento territorial Jóvenes en Movimiento, Tamaulipas."
    },
    {
        "Del año": "2001",
        "Al año": "2001",
        "Experiencia": "Diseñadora del Plan de Evaluación de Estructuras de Operación Política del PRI en Tamaulipas."
    },
    {
        "Del año": "1999",
        "Al año": "1999",
        "Experiencia": "Curso básico de capacitación política juvenil, por el CDE del PRI en Tamaulipas."
    },
    {
        "Del año": "1998",
        "Al año": "",
        "Experiencia": "Consejera municipal del PRI en Nuevo Laredo, Tamaulipas."
    },
    {
        "Del año": "1996",
        "Al año": "1996",
        "Experiencia": "Cursos de Tópicos de Administración, Organización y Estrategia del PRI, por el CDM del PRI en Nuevo Laredo, Tamaulipas."
    },
    {
        "Del año": "1994",
        "Al año": "",
        "Experiencia": "Miembro activo del PRI."
    },
    {
        "Del año": "",
        "Al año": "",
        "Experiencia": "Integrante de asambleas nacionales del PRI."
    },
    {
        "Del año": "",
        "Al año": "",
        "Experiencia": "Comisionada nacional de la Comisión de Presupuesto y Fiscalización del CPN del PRI."
    },
    {
        "Del año": "",
        "Al año": "",
        "Experiencia": "Consejera municipal del PRI."
    },
    {
        "Del año": "",
        "Al año": "",
        "Experiencia": "Secretaria de gestión social del Frente Juvenil Revolucionario en Tamaulipas."
    },
    {
        "Del año": "",
        "Al año": "",
        "Experiencia": "Secretaria técnica de la Comisión Municipal de Gestión Social del CDM PRI en Nuevo Laredo, Tamaulipas."
    },
    {
        "Del año": "",
        "Al año": "",
        "Experiencia": "Secretario técnico de la Comisión Municipal de Asuntos de Mujeres en el CDM del PRI en Nuevo Laredo, Tamaulipas."
    },
    {
        "Del año": "",
        "Al año": "",
        "Experiencia": "Presidenta de la generación revolucionaria Luis Donaldo Colosio en Tamaulipas."
    },
    {
        "Del año": "",
        "Al año": "",
        "Experiencia": "Miembro del Organismo Nacional de Mujeres Priístas (ONMPRI) en Tamaulipas."
    },
    {
        "Del año": "",
        "Al año": "",
        "Experiencia": "Consejera estatal del PRI en Tamaulipas."
    },
    {
        "Del año": "TRAYECTORIA ACADÉMICA",
        "Al año": "",
        "Experiencia": ""
    },
    {
        "Del año": "2005",
        "Al año": "2007",
        "Experiencia": "Maestría en Administración de Negocios por la Texas A&M Internacional University."
    },
    {
        "Del año": "2003",
        "Al año": "2003",
        "Experiencia": "Curso de Desarrollo Sostenible, por especialistas del PNUMA de la ONU."
    },
    {
        "Del año": "2002",
        "Al año": "2002",
        "Experiencia": "Curso de Derecho Electoral, por el IFE, en Nuevo Laredo, Tamaulipas."
    },
    {
        "Del año": "2001",
        "Al año": "",
        "Experiencia": "Curso: Profesional en Aplicaciones de Cómputo."
    },
    {
        "Del año": "2000",
        "Al año": "",
        "Experiencia": "Curso: English a Second Language Course II."
    },
    {
        "Del año": "1997",
        "Al año": "2001",
        "Experiencia": "Licenciatura en Contaduría Pública por la Universidad Autónoma de Tamaulipas."
    },
    {
        "Del año": "OTROS RUBROS",
        "Al año": "",
        "Experiencia": ""
    },
    {
        "Del año": "2007",
        "Al año": "2007",
        "Experiencia": "Reconocimiento por la Universidad Autónoma de Tamaulipas al otorgar a su generación el nombre de: Generación C.P.A. Cristabell Zamora Cabrera."
    },
    {
        "Del año": "2003",
        "Al año": "2003",
        "Experiencia": "Ponente en la conferencia: ¿Dónde está el Nacionalismo?."
    },
    {
        "Del año": "2003",
        "Al año": "2003",
        "Experiencia": "Candidata a la medalla Luis Donaldo Colosio otorgada por el CEN del PRI."
    },
    {
        "Del año": "2002",
        "Al año": "2002",
        "Experiencia": "Reconocimiento por el Centro Internacional de Estudios del Río Bravo, por apoyar las actividades ecológicas como El Día del Río."
    },
    {
        "Del año": "2002",
        "Al año": "2003",
        "Experiencia": "Reconocimiento como conferencista en la Semana Internacional de Ciencia y Tecnología del Cutis #234."
    },
    {
        "Del año": "2001",
        "Al año": "2001",
        "Experiencia": "Publicación del artículo: MT. La Gran Estructura."
    },
    {
        "Del año": "2000",
        "Al año": "2000",
        "Experiencia": "Publicación del artículo: Política vs Valores."
    },
    {
        "Del año": "2000",
        "Al año": "2000",
        "Experiencia": "Publicación del artículo: ¿Dónde están las Nuevas Caras?."
    },
    {
        "Del año": "1999",
        "Al año": "1999",
        "Experiencia": "Ponente en la conferencia: Los jóvenes y los valores."
    },
    {
        "Del año": "1997",
        "Al año": "1997",
        "Experiencia": "Ganadora del campeonato estatal de oratoria 87 años de la Revolución."
    },
    {
        "Del año": "1997",
        "Al año": "1997",
        "Experiencia": "Reconocimiento de la logia masónica Orión Nº 19 por su trayectoria en oratoria, tras haber obtenido el primer y segundo lugar en 22 diferentes concursos a nivel municipal, regional y estatal."
    },
    {
        "Del año": "1997",
        "Al año": "1997",
        "Experiencia": "Reconocimiento de la Secretaría de Capacitación de la Confederación Nacional de Oradores Mexicanos."
    }
]
