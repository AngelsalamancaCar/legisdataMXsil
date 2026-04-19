En este documento se integra el analisis de variables que se desarrolllo para el diseno del etl de legisdatamx. 

  trayectoria_administrativa — Analysis

  Structure

  3 fields per entry: Del año, Al año, Experiencia (free text).

  Data quality:

  ┌─────────────────────────────────────────┬─────────┐
  │                  Issue                  │  Count  │
  ├─────────────────────────────────────────┼─────────┤
  │ Empty Experiencia                       │ 14,198  │
  ├─────────────────────────────────────────┼─────────┤
  │ Header leaks (Del año = "TRAYECTORIA…") │ 11,520  │
  ├─────────────────────────────────────────┼─────────┤
  │ Missing both years (but valid text)     │ 18,853  │
  ├─────────────────────────────────────────┼─────────┤
  │ Academic entries contaminating field    │ ~11,185 │
  ├─────────────────────────────────────────┼─────────┤
  │ Usable clean entries                    │ ~72,985 │
  └─────────────────────────────────────────┴─────────┘

  Records with data: 4,018 / 5,000 total. Entries per record: avg ~18.

  ---
  Critical difference from comisiones

  comisiones had 485 controlled vocabulary names — exact mapping was
  possible. trayectoria_administrativa has 60,104 unique values out of
  72,985 entries — it is fully free text. Direct name mapping is
  impossible. Feature extraction must be pattern/keyword based.

  ---
  Role hierarchy present in the data

  ┌──────────────────────┬───────┬─────────────────────┐
  │         Role         │ Count │ Hierarchical level  │
  ├──────────────────────┼───────┼─────────────────────┤
  │ secretario/a         │ 6,990 │ executive           │
  ├──────────────────────┼───────┼─────────────────────┤
  │ miembro/integrante   │ 7,691 │ membership          │
  ├──────────────────────┼───────┼─────────────────────┤
  │ coordinador/a        │ 4,679 │ mid-management      │
  ├──────────────────────┼───────┼─────────────────────┤
  │ consejero/a          │ 3,298 │ advisory            │
  ├──────────────────────┼───────┼─────────────────────┤
  │ director/a           │ 3,777 │ senior mgmt         │
  ├──────────────────────┼───────┼─────────────────────┤
  │ delegado/a           │ 1,972 │ field               │
  ├──────────────────────┼───────┼─────────────────────┤
  │ asesor/a             │ 1,948 │ advisory            │
  ├──────────────────────┼───────┼─────────────────────┤
  │ diputado local       │ 1,772 │ legislative         │
  ├──────────────────────┼───────┼─────────────────────┤
  │ director_general     │ 1,178 │ executive           │
  ├──────────────────────┼───────┼─────────────────────┤
  │ jefe/a               │ 1,282 │ mid-mgmt            │
  ├──────────────────────┼───────┼─────────────────────┤
  │ representante        │ 1,223 │ representative      │
  ├──────────────────────┼───────┼─────────────────────┤
  │ presidente/a         │ 920   │ leadership          │
  ├──────────────────────┼───────┼─────────────────────┤
  │ regidor/a            │ 905   │ municipal           │
  ├──────────────────────┼───────┼─────────────────────┤
  │ subsecretario/a      │ 697   │ sub-executive       │
  ├──────────────────────┼───────┼─────────────────────┤
  │ fundador/a           │ 709   │ founder             │
  ├──────────────────────┼───────┼─────────────────────┤
  │ gerente              │ 629   │ management          │
  ├──────────────────────┼───────┼─────────────────────┤
  │ vicepresidente       │ 391   │ leadership          │
  ├──────────────────────┼───────┼─────────────────────┤
  │ dirigente            │ 400   │ leadership          │
  ├──────────────────────┼───────┼─────────────────────┤
  │ presidente municipal │ 131   │ highest municipal   │
  ├──────────────────────┼───────┼─────────────────────┤
  │ síndico              │ 126   │ municipal oversight │
  └──────────────────────┴───────┴─────────────────────┘

  Institution type frequencies

  ┌─────────────────────────────────┬────────┐
  │              Type               │ Count  │
  ├─────────────────────────────────┼────────┤
  │ party (partido, PRI, PAN, PRD…) │ 16,286 │
  ├─────────────────────────────────┼────────┤
  │ university                      │ 5,528  │
  ├─────────────────────────────────┼────────┤
  │ secretaría de estado            │ 3,103  │
  ├─────────────────────────────────┼────────┤
  │ gobierno municipal              │ 1,620  │
  ├─────────────────────────────────┼────────┤
  │ sindicato                       │ 1,030  │
  ├─────────────────────────────────┼────────┤
  │ cámara de diputados             │ 421    │
  ├─────────────────────────────────┼────────┤
  │ gobierno del estado             │ 157    │
  ├─────────────────────────────────┼────────┤
  │ senado                          │ 185    │
  └─────────────────────────────────┴────────┘

  ---
  Proposed ML features for trayectoria_administrativa

  Since free text rules out name mapping, features are keyword-pattern
  binary flags:

  Role level flags (max role ever held — hierarchical signal):

  ┌──────────────────────┬───────────────────────────────────┐
  │        Column        │              Pattern              │
  ├──────────────────────┼───────────────────────────────────┤
  │ fue_presidente_mun   │ presidenta? municipal             │
  ├──────────────────────┼───────────────────────────────────┤
  │ fue_presidente_org   │ ^\s*presidenta?\b (not municipal) │
  ├──────────────────────┼───────────────────────────────────┤
  │ fue_director_general │ directora?\s+general              │
  ├──────────────────────┼───────────────────────────────────┤
  │ fue_secretario_cargo │ ^\s*secretari[ao]\b               │
  ├──────────────────────┼───────────────────────────────────┤
  │ fue_subsecretario    │ subsecretari[ao]                  │
  ├──────────────────────┼───────────────────────────────────┤
  │ fue_director         │ ^\s*directora?\b                  │
  ├──────────────────────┼───────────────────────────────────┤
  │ fue_coordinador      │ ^\s*coordinador[a]?\b             │
  ├──────────────────────┼───────────────────────────────────┤
  │ fue_delegado         │ ^\s*delegad[ao]\b                 │
  ├──────────────────────┼───────────────────────────────────┤
  │ fue_asesor           │ ^\s*asesor[a]?\b                  │
  ├──────────────────────┼───────────────────────────────────┤
  │ fue_regidor          │ regidor[a]?                       │
  ├──────────────────────┼───────────────────────────────────┤
  │ fue_sindico          │ síndico                           │
  └──────────────────────┴───────────────────────────────────┘

  Institution type flags:

  ┌───────────────────────┬─────────────────────────────────────────────┐
  │        Column         │                   Pattern                   │
  ├───────────────────────┼─────────────────────────────────────────────┤
  │ admin_en_partido      │ \bpartido\b, PRI/PAN/PRD/MORENA/PVEM/PT/MC  │
  │                       │ keyword                                     │
  ├───────────────────────┼─────────────────────────────────────────────┤
  │ admin_en_sindicato    │ sindicato, CTM, CROM, CNTE, SNTE, FSTSE     │
  ├───────────────────────┼─────────────────────────────────────────────┤
  │ admin_en_universidad  │ universidad, UNAM, IPN, ITESM               │
  ├───────────────────────┼─────────────────────────────────────────────┤
  │ admin_en_gobierno_fed │ secretaría de, gobierno federal,            │
  │                       │ SEP/IMSS/ISSSTE etc.                        │
  ├───────────────────────┼─────────────────────────────────────────────┤
  │ admin_en_gobierno_est │ gobierno del estado, gobernador             │
  ├───────────────────────┼─────────────────────────────────────────────┤
  │ admin_en_gobierno_mun │ municipio, presidente municipal,            │
  │                       │ ayuntamiento                                │
  └───────────────────────┴─────────────────────────────────────────────┘

  Ordinal seniority proxy (nivel_cargo_max: 0–5):
  0 = no entries
  1 = miembro/integrante/asesor/vocal
  2 = coordinador/delegado/representante/regidor
  3 = director/jefe/gerente/subdirector
  4 = director_general/secretario/subsecretario
  5 = presidente_org/vicepresidente/presidente_municipal/síndico

  ---
  trayectoria_administrativa — Juventud / Liderazgo Juvenil

  Scale

  1,601 entries with youth-related keywords across 1,482 unique texts,
  across all 10 legislatures.

  Keyword universe found

  ┌────────────────────────────┬──────────────────────────────┐
  │            Term            │         Observations         │
  ├────────────────────────────┼──────────────────────────────┤
  │ juventud/juvenil/juveniles │ overwhelmingly dominant      │
  ├────────────────────────────┼──────────────────────────────┤
  │ jóvenes/joven              │ present                      │
  ├────────────────────────────┼──────────────────────────────┤
  │ infancia/infante/infantil  │ child-welfare institutions   │
  ├────────────────────────────┼──────────────────────────────┤
  │ niñez/menor(es)            │ tutelary/social work context │
  ├────────────────────────────┼──────────────────────────────┤
  │ adolescen*                 │ minor policy contexts        │
  └────────────────────────────┴──────────────────────────────┘

  Three semantically distinct clusters

  Cluster A — Party youth wings (strongest political signal, ~60% of
  entries):
  - Frente Juvenil Revolucionario del PRI — by far the most common
  - Movimiento Nacional de la Juventud Revolucionaria (MNJR) — PRI
  - Vanguardia Juvenil Agrarista (CNC/PRI agro wing)
  - Secretaría Nacional/Estatal de Acción Juvenil del PAN
  - Asuntos de la Juventud del PVEM
  - Secretaria Juvenil del PRD/PST/Convergencia

  Cluster B — Government youth institutions (~20%):
  - Instituto Mexicano de la Juventud (IMJUVE — federal)
  - Causa Joven (federal program)
  - CREA (Consejo Nacional de Recursos para la Atención de la Juventud)
  - State-level: Instituto de la Juventud y el Deporte (Tabasco,
  Aguascalientes, etc.)
  - Instituto Nacional de la Juventud Mexicana

  Cluster C — Child/minor welfare (~15%, different from liderazgo juvenil):
  - Instituto Tutelar de Menores
  - Tribunal Tutelar de Menores
  - Centro de Integración Juvenil
  - Instituto de Protección a la Infancia
  - CENDI (Centro de Desarrollo Infantil)
  - Hospital Infantil

  These are judicial/social protection roles for minors — not youth
  leadership, should be flagged separately.

  Role distribution within juventud entries

  ┌──────────────────┬───────┬──────────────────┐
  │       Role       │ Count │ Liderazgo signal │
  ├──────────────────┼───────┼──────────────────┤
  │ otro (free-form) │ 795   │ mixed            │
  ├──────────────────┼───────┼──────────────────┤
  │ secretario/a     │ 322   │ ★★ cargo         │
  ├──────────────────┼───────┼──────────────────┤
  │ coordinador/a    │ 182   │ ★★ cargo         │
  ├──────────────────┼───────┼──────────────────┤
  │ director/a       │ 87    │ ★★★ liderazgo    │
  ├──────────────────┼───────┼──────────────────┤
  │ dirigente        │ 64    │ ★★★ liderazgo    │
  ├──────────────────┼───────┼──────────────────┤
  │ delegado/a       │ 57    │ ★ participación  │
  ├──────────────────┼───────┼──────────────────┤
  │ presidente/a     │ 36    │ ★★★ liderazgo    │
  ├──────────────────┼───────┼──────────────────┤
  │ fundador/a       │ 20    │ ★★★ liderazgo    │
  ├──────────────────┼───────┼──────────────────┤
  │ asesor/a         │ 19    │ ★ técnico        │
  ├──────────────────┼───────┼──────────────────┤
  │ representante    │ 19    │ ★ participación  │
  └──────────────────┴───────┴──────────────────┘

  Proposed liderazgo juvenil variables

  tiene_exp_juvenil — binary: any entry matches youth keyword pattern.

  lider_juvenil_partido — binary: held a leadership role in a party youth
  wing.
  Pattern: youth keyword AND (presidenta?, secretario general, director,
  coordinador general, dirigente, fundador, subsecretario) AND
  (PRI/PAN/PRD/MORENA/PVEM…)

  lider_juvenil_gobierno — binary: held a named role in a government youth
  institute.
  Pattern: (director, subdirector, coordinador, titular, secretario) AND
  (Instituto.*Juventud, IMJUVE, Causa Joven, CREA, CENDI exclusion)

  miembro_org_juvenil — binary: membership/participation only (not
  leadership).
  Pattern: youth keyword AND (miembro, integrante, militante, participó,
  brigadista, ponente, curso, taller) — explicitly not a leadership term.

  exp_menores_infancia — binary: child welfare/tutelary context. Pattern:
  (tutelar, infancia, infantil, CENDI, hospital infantil) AND (menores,
  niñez). Separate from liderazgo juvenil.

  nivel_liderazgo_juvenil — ordinal 0–3:
  0 = sin experiencia juvenil
  1 = participación (miembro, militante, ponente, curso)
  2 = cargo (secretario, coordinador, delegado, representante)
  3 = liderazgo (presidente, director, dirigente, fundador,
  coordinador_nacional/general)

  ---
  Implementation notes

  All these are implemented in transform.py in a new
  _extract_trayectoria_admin() function using re.search over the
  Experiencia text field. The Del año/Al año fields are available for
  future time-series features but are skipped for now.

  Filter before processing: skip entries where Experiencia is empty OR Del
  año contains "TRAYECTORIA" (header leak artifact).


comisiones — Analysis

  Structure — 5 fields per record, all 20,807 entries complete (no missing
  keys):

  ┌─────────────────────────────┬────────────────────────────────┐
  │            Field            │             Notes              │
  ├─────────────────────────────┼────────────────────────────────┤
  │ Comisión                    │ 485 unique raw names           │
  ├─────────────────────────────┼────────────────────────────────┤
  │ Puesto                      │ 5 values: see below            │
  ├─────────────────────────────┼────────────────────────────────┤
  │ Fecha Inicial / Fecha Final │ DD/MM/YYYY strings             │
  ├─────────────────────────────┼────────────────────────────────┤
  │ Estatus                     │ Activo (15,670) / Baja (5,137) │
  └─────────────────────────────┴────────────────────────────────┘

  Puesto distribution:

  ┌────────────────┬────────┐
  │     Value      │ Count  │
  ├────────────────┼────────┤
  │ Integrante     │ 14,519 │
  ├────────────────┼────────┤
  │ Secretario     │ 5,372  │
  ├────────────────┼────────┤
  │ Presidente     │ 899    │
  ├────────────────┼────────┤
  │ Vicepresidente │ 15     │
  ├────────────────┼────────┤
  │ Copresidente   │ 2      │
  └────────────────┴────────┘

  Body suffix distribution:

  ┌─────────────────────────┬──────────────────────────────────────┐
  │          Body           │                Count                 │
  ├─────────────────────────┼──────────────────────────────────────┤
  │ C. Diputados            │ 19,520                               │
  ├─────────────────────────┼──────────────────────────────────────┤
  │ Com. Perm.              │ 637 (+ variants with embedded dates) │
  ├─────────────────────────┼──────────────────────────────────────┤
  │ H. Congreso de la Unión │ 227                                  │
  ├─────────────────────────┼──────────────────────────────────────┤
  │ C. Senadores            │ 2                                    │
  └─────────────────────────┴──────────────────────────────────────┘

  Commission type (prefix):

  ┌────────────────────────────┬────────┐
  │            Type            │ Count  │
  ├────────────────────────────┼────────┤
  │ (regular, no prefix)       │ 18,068 │
  ├────────────────────────────┼────────┤
  │ Especial                   │ 1,960  │
  ├────────────────────────────┼────────┤
  │ Comité                     │ 619    │
  ├────────────────────────────┼────────┤
  │ Bicamaral                  │ 153    │
  ├────────────────────────────┼────────┤
  │ Comisión (explicit prefix) │ 7      │
  └────────────────────────────┴────────┘

  ---
  Normalization layers needed

  Layer 1 — Whitespace cleanup (no decisions, ~40 names): double space
  before ( in raw data. 'Salud  (C. Diputados)' → 'Salud (C. Diputados)'.
  Affects 34 distinct names.

  Layer 2 — Structural decomposition (no decisions): split raw name into 3
  parts:
  - nombre_comision — topic name, body suffix stripped
  - cuerpo_legislativo — C. Diputados / Com. Perm. / H. Congreso / C.
  Senadores
  - tipo_comision — regular / Especial / Comité / Bicamaral

  Layer 3 — Canonical name mapping (decisions needed): same thematic
  committee renamed across legislatures. Main groups:

  ┌──────────────────────────────┬─────────────────────────────────────┐
  │   Canonical (most recent)    │      Absorbed historical names      │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Igualdad de Género           │ Equidad y Género                    │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Ciencia, Tecnología e        │ Ciencia y Tecnología                │
  │ Innovación                   │                                     │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Medio Ambiente y Recursos    │ Ecología y Medio Ambiente, Medio    │
  │ Naturales                    │ Ambiente, Sustentabilidad, Cambio   │
  │                              │ Climático y Recursos Naturales      │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Cambio Climático y           │ Cambio Climático                    │
  │ Sostenibilidad               │                                     │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Recursos Hidráulicos, Agua   │ Recursos Hidráulicos, Agua Potable  │
  │ Potable y Saneamiento        │ y Saneamiento, Asuntos Hidráulicos  │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Pueblos Indígenas y          │ Asuntos Indígenas, Pueblos          │
  │ Afromexicanos                │ Indígenas                           │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Derechos de la Niñez y       │ Derechos de la Niñez                │
  │ Adolescencia                 │                                     │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Economía, Comercio y         │ Economía, Comercio, Comercio y      │
  │ Competitividad               │ Fomento Industrial, Competitividad  │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Economía Social y Fomento    │ Fomento Cooperativo, Fomento        │
  │ del Cooperativismo           │ Cooperativo y Economía Social       │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Comunicaciones y Transportes │ Comunicaciones, Transportes         │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Cultura y Cinematografía     │ Cultura                             │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Radio, Televisión y          │ Radio y Televisión                  │
  │ Cinematografía               │                                     │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Asuntos Migratorios          │ Población, Fronteras y Asuntos      │
  │                              │ Migratorios, Población y Desarrollo │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │                              │ Gobernación, Gobernación y          │
  │ Gobernación y Población      │ Seguridad Pública, Gobernación y    │
  │                              │ Puntos Constitucionales             │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Presupuesto y Cuenta Pública │ Programación, Presupuesto y Cuenta  │
  │                              │ Pública                             │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Vigilancia de la Auditoría   │ Vigilancia de la Contaduría Mayor   │
  │ Superior de la Federación    │ de Hacienda                         │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Régimen, Reglamentos y       │ Reglamentos y Prácticas             │
  │ Prácticas Parlamentarias     │ Parlamentarias                      │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Protección Civil y           │ Protección Civil                    │
  │ Prevención de Desastres      │                                     │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Desarrollo Urbano y          │ Asentamientos Humanos y Obras       │
  │ Ordenamiento Territorial     │ Públicas                            │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Desarrollo Metropolitano,    │ Desarrollo Metropolitano,           │
  │ Urbano, Ordenamiento         │ Desarrollo Urbano y Ordenamiento    │
  │ Territorial y Movilidad      │ Territorial, Movilidad              │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Fortalecimiento del          │ Fortalecimiento al Federalismo      │
  │ Federalismo                  │                                     │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Federalismo y Desarrollo     │ Desarrollo Municipal                │
  │ Municipal                    │                                     │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Comité de Información,       │ Información Gestoría y Quejas       │
  │ Gestoría y Quejas            │                                     │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Justicia                     │ Justicia y Derechos Humanos         │
  ├──────────────────────────────┼─────────────────────────────────────┤
  │ Ganadería                    │ Agricultura y Ganadería (partially  │
  │                              │ — also split to Agricultura)        │
  └──────────────────────────────┴─────────────────────────────────────┘

  Layer 4 — ML feature generation (options, see below)

  ---
  Decision points needed

  1. Estatus Baja (5,137 records)
  Include bajas or filter to Activo only? Bajas = the legislator left/was
  removed from the commission mid-term. Filtering them could miss relevant
  signal (e.g., fired from a commission is itself informative).

  2. Especial commissions (1,960 records)
  Three options:
  - A Keep with tipo=Especial label, count separately
  (n_comisiones_especiales)
  - B Map each Especial de X to parent committee X (e.g., Especial de
  Ganadería → Ganadería)
  - C Drop entirely — transient bodies, low signal

  3. Com. Perm. sessions (637 records)
  These are joint sessions when Congress is in recess (Jan–Feb, May–Aug),
  not thematic committees. Options:
  - A Drop — these are calendar artifacts, not committee assignments
  - B Count as n_sesiones_perm feature, keep thematic name

  4. Canonical direction for renamed committees
  Use most recent name (current legislature) as canonical? Or most
  frequent? Matters for models that need to compare across legislatures.

  5. ML output format — what features does transform.py generate?
  - Option A Binary flags: comision_hacienda=1, comision_salud=1, etc. (~40
   columns for all committees with count >50 in dataset). Can also add
  presidente_{name}.
  - Option B Count + role: keep existing n_comisiones, n_presidencias and
  add lista_comisiones_canon as a categorical list (for embedding
  downstream)
  - Option C Thematic groupings: cluster 40+ canonical names into ~8 policy
   domains (economia, social, seguridad, ambiente, gobernacion, etc.) then
  generate flags per domain

  6. Puesto collapse
  Vicepresidente (15) and Copresidente (2) are negligible. Collapse into
  Presidente?

  ---
  What are your decisions on these 6 points?

✻ Crunched for 3m 5s

❯ 1. forget estatus and do not use as filter. 2. especial comisions should
   be kept as option A. 3. count as n_sesiones_perm. 4. use most recent
  name. 5. add binary flags for the comisiones. 6. collapse into
  presidente_comision binary.


Proposed mapping: specific → canonical

  → Derecho (clear):
  Abogado, Derecho Constitucional, Derecho Constitucional y Amparo, Derecho Fiscal, Derecho Electoral, Derecho Parlamentario, Derecho Administrativo y
  Fiscal, Derecho Procesal Penal, Derecho Procesal, Derecho Laboral, Derecho Penal, Derecho Internacional, Derecho Comparado, Derecho Público, Derecho
  Constitucional Mexicano, Derecho Constitucional y Gobernabilidad, Derecho y Asuntos Internacionales, Derecho y Ciencias Sociales, Derecho Internacional y
   Finanzas Internacionales, Derecho con Orientación en Derecho de Amparo, Derecho Administrativo Sancionador Electoral, Derecho Procesal Constitucional,
  Derecho Económico Internacional, Derecho y Finanzas, Ciencias Jurídicas, Ciencias Jurídico Penales, Ciencias Penales, Juicios Orales, Métodos Alternos de
   Solución de Controversias, Mediación y Solución Colaborativa de Conflictos, Política Criminal, Procuración y Administración de Justicia

  → Administración y Contaduría (needs decision — see below):
  Administración, Administración de Empresas, Administración de Negocios, Administración de Negocios Internacionales, Administración Estratégica,
  Administración Internacional, Administración de Recursos Humanos, Administración Naval, Administración Militar para la Seguridad y Defensa Nacional,
  Administración de Proyectos de Inversión, Alta Dirección, Alta Dirección de Empresas, Contaduría, Contaduría Pública, Contaduría Administrativa,
  Contaduría Pública y Finanzas, Contador Público, Contador Público y Auditor, Contabilidad, Contabilidad y Administración Empresarial, Finanzas, Finanzas
  Públicas, Finanzas y Dirección, Auxiliar Contable, Mercadotecnia, Mercadotecnia Internacional, Mercadotecnia y Ventas, Comercio, Comercio Internacional,
  Negocios Internacionales, Gestión y Administración, Capital Humano, Gestión de Recursos Humanos, Dirección de Empresas, Dirección de Empresas para
  Ejecutivos con Experiencia, Dirección Estratégica de Empresas Familiares, Dirección Estratégica y Gestión de la Innovación, Calidad Total, Calidad Total
  y Competitividad, Valuación, Auditoría Gubernamental, Ciencias de la Administración, Ciencias Administrativas, Informática Contable, Administración y
  Mercadotecnia

  → Ciencias Políticas y Sociales:
  Ciencias Políticas, Ciencias Políticas y Administración Pública, Sociología, Relaciones Internacionales, Asuntos Internacionales, Políticas Públicas,
  Gobierno y Políticas Públicas, Gobierno y Administración, Gobierno y Administración Pública, Estudios Políticos y Gobierno, Estudios Latinoamericanos,
  Estudios Regionales, Estudios Comparativos de Política, Estudios de Población, Acción Política, Ciencias Sociales, Trabajo Social, Relaciones Públicas,
  Derechos Humanos, Derechos Humanos y Garantías, Gobernanza y Derechos Humanos, Gobernanza y Globalización, Gobernanza y Gobiernos Locales, Desarrollo
  Regional, Desarrollo Comunitario, Desarrollo Económico Regional, Responsabilidad Social, Seguridad Nacional, Gerencia Política, Práctica Política,
  Comunicación Política, Comunicación Política y Gobernanza Estratégica, Intervención Social en las Sociedades del Conocimiento, Estudios de las Mujeres,
  Género y Ciudadanía, Ciencias Políticas y Gestión Pública, Política y Gestión Pública, Política y Gestión Social, Estudios Parlamentarios, Ciencias
  Políticas y Administración Urbana, Economía y Gobierno, Economía y Buen Gobierno, Economía y Política Pública

  → Ciencias de la Educación:
  Educación, Educación Primaria, Educación Básica, Educación Media, Educación Preescolar, Educación Internacional, Educación Primaria para el Medio
  Indígena, Profesora de Educación Primaria, Desarrollo de la Educación Básica, Pedagogía, Docencia en Educación Superior, Innovación Educativa, Gestión
  Educativa, Gestión en la Educación Superior, Gestión del Aprendizaje, Administración Educativa, Desarrollo Cognitivo de las Inteligencias Múltiples,
  Programación Neurolingüística, Educador Físico, Ciencias de la Familia y Educación, Ciencias Sociales de la Educación, Educación Relacional y
  Bioaprendizaje, Educación Media en el Área de Ciencias Naturales

  → Ingeniería:
  Ingeniería Civil, Ingeniería Industrial, Ingeniería Industrial y de Sistemas, Ingeniería Mecánica, Ingeniería Mecánica y Eléctrica, Ingeniería
  Aeroespacial, Ingeniería Biomédica, Ingeniería Petrolera, Ingeniería Administrativa, Ingeniería en Sistemas Computacionales, Ingeniería de Sistemas,
  Ingeniería Forestal, Computación, Computación Administrativa, Ciencias de la Computación, Informática, Sistemas Computarizados e Informática, Sistemas de
   Computación Administrativa, Programación, Administración de Tecnologías de la Información

  → Económico-Financiera:
  Economía, Economía del Empleo, Economía, Pobreza y Desarrollo Social

  → Ciencias de la Salud:
  Médico Cirujano, Médico Cirujano y Partero, Medicina, Medicina Tradicional China y Moxibustión, Enfermería, Enfermería y Obstetricia, Nutrición,
  Nutrición Clínica, Odontología, Gerontología, Químico Farmacobiólogo, Química, Administración en Sistemas de Salud, Administración de Hospitales,
  Administración de Instituciones de Salud

  → Psicología:
  Psicología Organizacional, Psicología Moderna, Psicoterapias Humanistas

  → Comunicación:
  Ciencias de la Comunicación, Comunicación Organizacional, Comunicación Social, Comunicación y Cultura, Comunicación Social y Política, Ciencias de la
  Información y Comunicación, Diseño y Comunicación Visual, Locución

  → Humanidades:
  Historia, Historia del Arte, Literatura Española, Filosofía, Escritura

  → Arquitectura y Diseño:
  Arquitectura, Diseño Industrial, Diseño de Modas

  → Agropecuaria y Zootecnia:
  Ingeniero Agrónomo, Ingeniero Agrónomo Zootecnista, Ingeniero Agrónomo Fitotecnista, Ingeniería Agrónoma Fitotecnista, Ingeniería Agrónoma en Producción,
   Ingeniería en Desarrollo Agrícola, Medicina Veterinaria y Zootecnia, Veterinaria y Zootecnista, Ciencias Forestales, Forestal, Manejo y Conservación de
  Bosques Tropicales y Biodiversidad, Desarrollo Sustentable y Ecoturismo

  → Administración Pública:
  Administración Pública Estatal y Municipal, Administración Pública y Políticas Públicas, Administración y Políticas Públicas, Administración y Políticas
  con Enfoque en Gestión Política, Administración y Gestión Electoral, Gestión Pública Aplicada, Gestión Pública Municipal, Gerencia Pública, Gobierno y
  Gestión Pública

  → Ciencias:
  Ciencias Naturales, Biología

  → Otra (catch-all for tourism, sports, etc.):
  Hotelería y Gastronomía, Turismo, Organización Deportiva, Gestión de Entidades Deportivas, Historia del Arte... and anything not matched

  ---
  Decision points needed from you

  1. Administración (26) vs Administración Pública (50)
  Both could be a canonical category. Currently Administración is free-text, Administración Pública is a SIL category. Merge Administración +
  Administración de Empresas → Administración y Contaduría, or keep Administración Pública separate and map generic Administración → Administración y
  Contaduría?

  2. Biología (3) + Química (1)
  → Ciencias de la Salud or → Ciencias? Biología is ambiguous.

  3. Economía y Gobierno, Economía y Política Pública etc.
  → Económico-Financiera or → Ciencias Políticas y Sociales?

  4. Overlapping fields like Derecho y Ciencias Sociales, Ciencias Jurídicas, Administrativas y de la Educación — assign to dominant domain (Derecho) or
  Otra?

  ---
  Implementation plan

  Add _normalize_preparacion() to clean.py that:
  1. Tries exact match first
  2. Falls back to keyword-based matching (e.g. "Derecho" in value → Derecho)
  3. Unknown → Otra
  4. Replaces n_palabras_preparacion (word count) with area_formacion (categorical) + optionally keep the word-count flag

  Should I implement this once you confirm the decisions above?

✻ Churned for 2m 13s

❯ Administracion should be merged with administracion de empresas and administracion y contaduria. administracion publica should be separate and grouped
  with ciencias politicas y sociales. Biologia and Quimica should be grouped in Ciencias as well as Ciencias de la salud. On decision 3 group them as
  ciencias politicas y sociales. Derecho should be grouped as Humanidades as well as ciencias juridicas. Educacion should be grouped with pedagogia in its
   own category due to its political rlevance. Implement a mapping according to these principles to keep transparent the decision in the code and follow
  the mapping you suggested. Adjust the implementation plan with this in step 2, but follow its general structure of the other steps.




experiencia_legislativa — Analysis

  Structure: space-concatenated list of prior roles. Gender variants but semantically identical. Only 3 distinct roles exist:

  ┌─────────────────────┬──────────────────────────────────────────────┐
  │ Role (M/F variants) │                   Meaning                    │
  ├─────────────────────┼──────────────────────────────────────────────┤
  │ Diputado/a Local    │ Prior state-level deputy                     │
  ├─────────────────────┼──────────────────────────────────────────────┤
  │ Diputado/a Federal  │ Prior federal deputy (different legislature) │
  ├─────────────────────┼──────────────────────────────────────────────┤
  │ Senador/a           │ Prior senator                                │
  └─────────────────────┴──────────────────────────────────────────────┘

  All 14 values are combinations of these 3 — no free text, no ambiguity.

  Coverage: 12%–57% per legislature (records with empty = zero prior experience, not missing data).

  ---
  No decisions needed — clear normalization

  The n_palabras_exp_legislativa word count is a noisy proxy (counts role-words × 2). Replace with proper binary flags:

  ┌───────────────────────────────┬────────────────────────────────────────────────────────┐
  │          New column           │                         Logic                          │
  ├───────────────────────────────┼────────────────────────────────────────────────────────┤
  │ fue_diputado_local            │ 1 if "Diputado Local" or "Diputada Local" in value     │
  ├───────────────────────────────┼────────────────────────────────────────────────────────┤
  │ fue_diputado_federal          │ 1 if "Diputado Federal" or "Diputada Federal" in value │
  ├───────────────────────────────┼────────────────────────────────────────────────────────┤
  │ fue_senador                   │ 1 if "Senador" or "Senadora" in value                  │
  ├───────────────────────────────┼────────────────────────────────────────────────────────┤
  │ n_cargos_legislativos_previos │ sum of above (0–3)                                     │
  └───────────────────────────────┴────────────────────────────────────────────────────────┘

  Empty/null → all zeros (no prior experience, not missing)


comisiones — Analysis

  Structure — 5 fields per record, all 20,807 entries complete (no missing keys):

  ┌─────────────────────────────┬────────────────────────────────┐
  │            Field            │             Notes              │
  ├─────────────────────────────┼────────────────────────────────┤
  │ Comisión                    │ 485 unique raw names           │
  ├─────────────────────────────┼────────────────────────────────┤
  │ Puesto                      │ 5 values: see below            │
  ├─────────────────────────────┼────────────────────────────────┤
  │ Fecha Inicial / Fecha Final │ DD/MM/YYYY strings             │
  ├─────────────────────────────┼────────────────────────────────┤
  │ Estatus                     │ Activo (15,670) / Baja (5,137) │
  └─────────────────────────────┴────────────────────────────────┘

  Puesto distribution:

  ┌────────────────┬────────┐
  │     Value      │ Count  │
  ├────────────────┼────────┤
  │ Integrante     │ 14,519 │
  ├────────────────┼────────┤
  │ Secretario     │ 5,372  │
  ├────────────────┼────────┤
  │ Presidente     │ 899    │
  ├────────────────┼────────┤
  │ Vicepresidente │ 15     │
  ├────────────────┼────────┤
  │ Copresidente   │ 2      │
  └────────────────┴────────┘

  Body suffix distribution:

  ┌─────────────────────────┬──────────────────────────────────────┐
  │          Body           │                Count                 │
  ├─────────────────────────┼──────────────────────────────────────┤
  │ C. Diputados            │ 19,520                               │
  ├─────────────────────────┼──────────────────────────────────────┤
  │ Com. Perm.              │ 637 (+ variants with embedded dates) │
  ├─────────────────────────┼──────────────────────────────────────┤
  │ H. Congreso de la Unión │ 227                                  │
  ├─────────────────────────┼──────────────────────────────────────┤
  │ C. Senadores            │ 2                                    │
  └─────────────────────────┴──────────────────────────────────────┘

  Commission type (prefix):

  ┌────────────────────────────┬────────┐
  │            Type            │ Count  │
  ├────────────────────────────┼────────┤
  │ (regular, no prefix)       │ 18,068 │
  ├────────────────────────────┼────────┤
  │ Especial                   │ 1,960  │
  ├────────────────────────────┼────────┤
  │ Comité                     │ 619    │
  ├────────────────────────────┼────────┤
  │ Bicamaral                  │ 153    │
  ├────────────────────────────┼────────┤
  │ Comisión (explicit prefix) │ 7      │
  └────────────────────────────┴────────┘

  ---
  Normalization layers needed

  Layer 1 — Whitespace cleanup (no decisions, ~40 names): double space before ( in raw data. 'Salud  (C. Diputados)' → 'Salud (C. Diputados)'. Affects 34
  distinct names.

  Layer 2 — Structural decomposition (no decisions): split raw name into 3 parts:
  - nombre_comision — topic name, body suffix stripped
  - cuerpo_legislativo — C. Diputados / Com. Perm. / H. Congreso / C. Senadores
  - tipo_comision — regular / Especial / Comité / Bicamaral

  Layer 3 — Canonical name mapping (decisions needed): same thematic committee renamed across legislatures. Main groups:

  ┌──────────────────────────────────────────────────────────────┬─────────────────────────────────────────────────────────────────────────────────────┐
  │                   Canonical (most recent)                    │                              Absorbed historical names                              │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Igualdad de Género                                           │ Equidad y Género                                                                    │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Ciencia, Tecnología e Innovación                             │ Ciencia y Tecnología                                                                │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Medio Ambiente y Recursos Naturales                          │ Ecología y Medio Ambiente, Medio Ambiente, Sustentabilidad, Cambio Climático y      │
  │                                                              │ Recursos Naturales                                                                  │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Cambio Climático y Sostenibilidad                            │ Cambio Climático                                                                    │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Recursos Hidráulicos, Agua Potable y Saneamiento             │ Recursos Hidráulicos, Agua Potable y Saneamiento, Asuntos Hidráulicos               │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Pueblos Indígenas y Afromexicanos                            │ Asuntos Indígenas, Pueblos Indígenas                                                │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Derechos de la Niñez y Adolescencia                          │ Derechos de la Niñez                                                                │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Economía, Comercio y Competitividad                          │ Economía, Comercio, Comercio y Fomento Industrial, Competitividad                   │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Economía Social y Fomento del Cooperativismo                 │ Fomento Cooperativo, Fomento Cooperativo y Economía Social                          │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Comunicaciones y Transportes                                 │ Comunicaciones, Transportes                                                         │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Cultura y Cinematografía                                     │ Cultura
  │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Radio, Televisión y Cinematografía                           │ Radio y Televisión                                                                  │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Asuntos Migratorios                                          │ Población, Fronteras y Asuntos Migratorios, Población y Desarrollo                  │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Gobernación y Población                                      │ Gobernación, Gobernación y Seguridad Pública, Gobernación y Puntos Constitucionales │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Presupuesto y Cuenta Pública                                 │ Programación, Presupuesto y Cuenta Pública                                          │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Vigilancia de la Auditoría Superior de la Federación         │ Vigilancia de la Contaduría Mayor de Hacienda                                       │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Régimen, Reglamentos y Prácticas Parlamentarias              │ Reglamentos y Prácticas Parlamentarias                                              │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Protección Civil y Prevención de Desastres                   │ Protección Civil                                                                    │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Desarrollo Urbano y Ordenamiento Territorial                 │ Asentamientos Humanos y Obras Públicas                                              │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Desarrollo Metropolitano, Urbano, Ordenamiento Territorial y │ Desarrollo Metropolitano, Desarrollo Urbano y Ordenamiento Territorial, Movilidad   │
  │  Movilidad                                                   │
          │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Fortalecimiento del Federalismo                              │ Fortalecimiento al Federalismo                                                      │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Federalismo y Desarrollo Municipal                           │ Desarrollo Municipal                                                                │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Comité de Información, Gestoría y Quejas                     │ Información Gestoría y Quejas                                                       │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Justicia                                                     │ Justicia y Derechos Humanos                                                         │
  ├──────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Ganadería                                                    │ Agricultura y Ganadería (partially — also split to Agricultura)                     │
  └──────────────────────────────────────────────────────────────┴─────────────────────────────────────────────────────────────────────────────────────┘

  Layer 4 — ML feature generation (options, see below)

  ---
  Decision points needed

  1. Estatus Baja (5,137 records)
  Include bajas or filter to Activo only? Bajas = the legislator left/was removed from the commission mid-term. Filtering them could miss relevant signal
  (e.g., fired from a commission is itself informative).

  2. Especial commissions (1,960 records)
  Three options:
  - A Keep with tipo=Especial label, count separately (n_comisiones_especiales)
  - B Map each Especial de X to parent committee X (e.g., Especial de Ganadería → Ganadería)
  - C Drop entirely — transient bodies, low signal

  3. Com. Perm. sessions (637 records)
  These are joint sessions when Congress is in recess (Jan–Feb, May–Aug), not thematic committees. Options:
  - A Drop — these are calendar artifacts, not committee assignments
  - B Count as n_sesiones_perm feature, keep thematic name

  4. Canonical direction for renamed committees
  Use most recent name (current legislature) as canonical? Or most frequent? Matters for models that need to compare across legislatures.

  5. ML output format — what features does transform.py generate?
  - Option A Binary flags: comision_hacienda=1, comision_salud=1, etc. (~40 columns for all committees with count >50 in dataset). Can also add
  presidente_{name}.
  - Option B Count + role: keep existing n_comisiones, n_presidencias and add lista_comisiones_canon as a categorical list (for embedding downstream)
  - Option C Thematic groupings: cluster 40+ canonical names into ~8 policy domains (economia, social, seguridad, ambiente, gobernacion, etc.) then
  generate flags per domain

  6. Puesto collapse
  Vicepresidente (15) and Copresidente (2) are negligible. Collapse into Presidente?

  ---
  What are your decisions on these 6 points?

✻ Crunched for 3m 5s

❯ 1. forget estatus and do not use as filter. 2. especial comisions should be kept as option A. 3. count as n_sesiones_perm. 4. use most recent name. 5.
  add binary flags for the comisiones. 6. collapse into presidente_comision binary.
