# 4. Datos disponibles (Diccionario de datos — legisdatamxsil)

Los datos provienen del Sistema de Información Legislativa (SIL) de la Secretaría de Gobernación de México ([sil.gobernacion.gob.mx](https://sil.gobernacion.gob.mx)). El raspador descarga los perfiles públicos de los diputados federales de la Cámara de Diputados del H. Congreso de la Unión, abarcando las legislaturas LVII a LXVI (1997–presente), con aproximadamente 500 diputados por legislatura.

Cada corrida del raspador produce un archivo CSV por legislatura. Cada archivo contiene una fila por perfil descargado y 32 columnas que recogen tanto los datos personales planos como las secciones de trayectoria y comisiones, serializadas como listas JSON dentro de una celda.

La unidad de análisis es el legislador dentro de una legislatura. Un mismo diputado puede aparecer en más de un archivo si fue reelecto en legislaturas distintas. El campo `diputado_id` permite vincular registros del mismo individuo entre legislaturas (se calcula como SHA-256[:12] sobre nombre normalizado y fecha de nacimiento).

---

## Columnas del CSV crudo (salida del raspador)

### Identificadores

| Campo | Tipo | Descripción |
|---|---|---|
| **diputado_id** | str | ID estable entre legislaturas: SHA-256[:12] calculado sobre nombre normalizado + fecha de nacimiento. Vacío si faltan nombre o nacimiento. |
| **referencia** | int | ID numérico del perfil en el SIL. Identifica de forma única a un legislador dentro de una legislatura, pero puede cambiar entre legislaturas. |
| **legislatura_num** | int | Número entero de la legislatura (57–66). Asignado por el raspador según la legislatura objetivo. |
| **profile_url** | str | URL directa al perfil del legislador en el SIL. Útil para diagnóstico y verificación manual. |

### Datos personales (tabla principal del perfil)

| Campo | Tipo | Descripción |
|---|---|---|
| **nombre** | str | Nombre completo del legislador extraído del encabezado del perfil. Los prefijos de cargo (Diputado/a, Sen., Lic., etc.) son eliminados automáticamente. |
| **numero_de_la_legislatura** | str | Número de la legislatura tal como aparece en el perfil (p. ej. "LXVI Legislatura"). |
| **periodo_de_la_legislatura** | str | Años del período legislativo (p. ej. "2024-2027"). |
| **partido** | str | Nombre o siglas del partido político al que pertenece el legislador según el SIL. |
| **nacimiento** | str | Fecha de nacimiento en formato DD/MM/YYYY. Puede estar vacía en perfiles incompletos. |
| **entidad** | str | Entidad federativa que representa o por la que fue electo. |
| **ciudad** | str | Ciudad de nacimiento o de referencia del legislador, según disponibilidad en el perfil. |
| **principio_de_eleccion** | str | Tipo de elección: mayoría relativa (MR) o representación proporcional (RP). |
| **ubicacion** | str | Distrito o circunscripción electoral. |
| **correo_electronico** | str | Correo institucional publicado en el perfil. Frecuentemente vacío o desactualizado. |
| **telefono** | str | Teléfono institucional publicado en el perfil. Frecuentemente vacío. |
| **suplente** | str | Nombre completo del suplente registrado. Vacío si no tiene suplente. |
| **suplente_referencia** | str | ID del perfil del suplente en el SIL. Vacío si no tiene suplente. |
| **ultimo_grado_de_estudios** | str | Nivel de escolaridad declarado (p. ej. "Licenciatura", "Maestría", "Doctorado"). Texto libre; varía en ortografía entre perfiles. |
| **preparacion_academica** | str | Campo de formación o carrera declarada. Texto libre (p. ej. "Derecho", "Ingeniería Civil"). |
| **experiencia_legislativa** | str | Texto libre que describe cargos legislativos previos. Puede incluir referencias a diputaciones locales, federales o senadurías anteriores. |
| **redes_sociales** | str | Cuentas de redes sociales publicadas en el perfil. Frecuentemente vacías o desactualizadas. |

### Secciones anidadas (serializadas como JSON)

Cada una de estas columnas contiene una lista de diccionarios serializada como cadena JSON. Los registros vacíos se guardan como `"[]"`. La mayoría de las secciones de trayectoria tienen la estructura `[{"Del año": "...", "Al año": "...", "Experiencia": "..."}, ...]`.

| Campo | Estructura JSON | Descripción |
|---|---|---|
| **comisiones** | `[{Comisión, Puesto, Fecha Inicial, Fecha Final, Estatus}]` | Membresías en comisiones legislativas (ordinarias, especiales, de investigación). Incluye el puesto (Integrante, Secretario, Presidente, etc.) y estatus (activo/concluido). |
| **licencias_reincorporaciones** | `[{Del año, Al año, Experiencia}]` | Registro de licencias tomadas y reincorporaciones al cargo durante la legislatura. |
| **trayectoria_administrativa** | `[{Del año, Al año, Experiencia}]` | Cargos en la administración pública (federal, estatal o municipal), organizaciones civiles, sindicatos, partidos, etc. |
| **trayectoria_legislativa** | `[{Del año, Al año, Experiencia}]` | Cargos legislativos previos (diputaciones locales o federales anteriores, senadurías). |
| **trayectoria_politica** | `[{Del año, Al año, Experiencia}]` | Participación en partidos políticos, candidaturas, militancia y cargos internos partidistas. |
| **trayectoria_academica** | `[{Del año, Al año, Experiencia}]` | Estudios realizados: institución, grado y período. Puede incluir cursos, diplomados y formación en el extranjero. |
| **trayectoria_empresarial** | `[{Del año, Al año, Experiencia}]` | Actividad en el sector privado o iniciativa privada: cargos directivos, empresas propias, consultoría. |
| **otros_rubros** | `[{Del año, Al año, Experiencia}]` | Actividades no clasificadas en las demás categorías. En la legislatura LXVI contiene además la subsección de investigación y docencia antes de que el raspador la separe. |
| **organos_de_gobierno** | `[{...}]` | Participación en órganos de gobierno de organismos autónomos, fideicomisos u otras instituciones. Estructura variable según el perfil. |
| **observaciones** | `[{...}]` | Notas adicionales del perfil. Estructura variable; frecuentemente vacío. |

### Diagnóstico

| Campo | Tipo | Descripción |
|---|---|---|
| **error** | str | Vacío si el perfil fue descargado y procesado sin incidencias. Contiene el mensaje de error (`fetch_failed`, etc.) si la descarga falló. Los perfiles con error tienen el resto de sus campos vacíos. |

---

## Knowns and Unknowns

**Conocido**: fuente de datos (SIL, público), cobertura temporal (LVII–LXVI, 1997–presente), unidad de análisis (legislador × legislatura), estructura de cada columna, mecanismo de reanudación automática ante interrupciones, método de generación del `diputado_id`.

**Desconocido / evidencia insuficiente**: tasa de perfiles con error por legislatura; completitud real de los campos de texto libre (`preparacion_academica`, `experiencia_legislativa`) dado que son declarativos y su llenado depende de cada legislador; posibles duplicados dentro de una misma legislatura si un legislador aparece en más de un partido (el raspador deduplica por referencia, pero el SIL podría asignar referencias distintas al mismo individuo); cobertura real de `licencias_reincorporaciones` en legislaturas anteriores a LXII; grado de estandarización de los campos de texto libre entre legislaturas.
