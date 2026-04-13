"""
generar_guia.py
---------------
Script para regenerar guia_visita_611.html con datos frescos.
Uso: python generar_guia.py

Requiere en la misma carpeta:
  - venta_mes_actual.xlsx   (sabana de ventas del mes en curso)
  - venta_mes_anterior.xlsx (sabana del mes anterior, para historial)
  - maestro_clientes.xlsx
  - cliente_zona.xlsx
  - guia_template.html      (el HTML base sin datos)

Genera:
  - guia_visita_611.html    (lista para distribuir)
"""

import pandas as pd
import json
import re
import math
import os
import sys
from datetime import datetime

print("=" * 60)
print("MAS Analytics - Generador de Guia de Visita")
print("=" * 60)

# ── CONFIGURACION ──────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Detectar archivos Excel automaticamente en la carpeta data/
DATA_DIR = os.path.join(BASE_DIR, "data")
xlsx_files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith('.xlsx') and not f.startswith('~')])
print(f"Archivos encontrados en data/: {xlsx_files}")

# El mas reciente es el mes actual, el anterior es el mes pasado
# Si hay solo uno, se usa para ambos (historial y actual)
if len(xlsx_files) == 0:
    print("ERROR: No hay archivos .xlsx en la carpeta data/")
    sys.exit(1)
elif len(xlsx_files) == 1:
    archivo_actual   = os.path.join(DATA_DIR, xlsx_files[0])
    archivo_anterior = os.path.join(DATA_DIR, xlsx_files[0])
    print(f"Un solo archivo detectado — se usa para mes actual y anterior: {xlsx_files[0]}")
else:
    # Ordenados por fecha de modificacion: el mas nuevo = actual, el anterior = historial
    xlsx_con_fecha = sorted(
        [(f, os.path.getmtime(os.path.join(DATA_DIR, f))) for f in xlsx_files],
        key=lambda x: x[1], reverse=True
    )
    archivo_actual   = os.path.join(DATA_DIR, xlsx_con_fecha[0][0])
    archivo_anterior = os.path.join(DATA_DIR, xlsx_con_fecha[1][0])
    print(f"Mes actual:   {xlsx_con_fecha[0][0]}")
    print(f"Mes anterior: {xlsx_con_fecha[1][0]}")

# Buscar maestro y cliente_zona (cualquier nombre que contenga esas palabras)
def find_file(keyword):
    matches = [f for f in xlsx_files if keyword.lower() in f.lower()]
    if matches:
        return os.path.join(DATA_DIR, matches[0])
    return None

maestro_auto    = find_file('maestro')
cliente_zona_auto = find_file('zona') or find_file('cliente_zona')

FILES = {
    "venta_actual":   archivo_actual,
    "venta_anterior": archivo_anterior,
    "maestro":        maestro_auto or archivo_actual,
    "cliente_zona":   cliente_zona_auto or archivo_actual,
    "template":       os.path.join(BASE_DIR, "guia_template.html"),
    "output":         os.path.join(BASE_DIR, "guia_visita_611.html"),
}

SUP_MAP = {}
for v in [41,45,49,52,53,55,56,57,59]: SUP_MAP[v] = 300
for v in [31,32,33,34,35,36,37,38,39,51]: SUP_MAP[v] = 400
for v in [42,43,44,46,47,48,54,58]: SUP_MAP[v] = 500

MARCAS = ['Lays','Doritos','Cheetos','3D','Pep','Pehuamar','Twistos','Tostitos','Quaker']
MARCAS_KW = {
    'Lays':['lays'], 'Doritos':['doritos'], 'Cheetos':['cheetos'],
    '3D':['3d'], 'Pep':['pep comun','pep rueditas','pep '],
    'Pehuamar':['pehuamar'], 'Twistos':['twistos'],
    'Tostitos':['tostitos'], 'Quaker':['quaker','avena']
}
DIA_MAP = {1:'Lunes',2:'Martes',3:'Miercoles',4:'Jueves',5:'Viernes',6:'Sabado'}
PROV_MAP = {
    'Pepsico de Argentina SRL':'PepsiCo','MOLINOS RIO DE LA PLATA SA':'Molinos',
    'SOFTYS ARGENTINA SA':'Softys','DIELO S.A.                      ':'Dielo',
    'GENOMMA LABORATORIES ARGENTINA SA':'Genomma','GEORGALOS HNOS S A I C A':'Georgalos',
    'DON SATUR SRL':'Don Satur','INDUSTRIAS QUIMICAS Y MINERAS TIMBO SA':'Timbo',
    'BUHL SA':'Buhl','TRES H BEBIDAS S.A.S.':'Tres H','DULCOR SA':'Dulcor',
    'JOSE LLENES SACIF':'Llenes','CRONI SA':'Croni',
    'COOPERATIVA AGRICOLA DE LA COLONIA LIEBIG LTDA':'Liebig',
}
EXCLUIR_PROV = {'PepsiCo','Molinos','Softys','Combos seisonce','Administracion 611'}
TODOS_PROV_ACTIVOS = ['Don Satur','Georgalos','Genomma','Timbo','Buhl',
                      'Tres H','Dulcor','Liebig','Llenes','Croni']

def si(v, d=0):
    try: return d if (v is None or (isinstance(v,float) and math.isnan(v))) else int(v)
    except: return d

def sf(v, d=0.0):
    try: return d if (v is None or (isinstance(v,float) and math.isnan(v))) else float(v)
    except: return d

def clean_str(s, maxlen=35):
    s = str(s).strip()
    try: s = s.encode('latin1').decode('utf-8')
    except: pass
    s = re.sub(r'[^\x20-\x7E]', '', s)
    return s[:maxlen]

def exkg(art):
    m = re.search(r'(\d+)\s*g(?:r|rs)?(?:\b|x)', str(art), re.I)
    if not m: m = re.search(r'(\d+)g', str(art).lower())
    return int(m.group(1))/1000 if m else 0

def get_prov(p):
    return PROV_MAP.get(str(p).strip(), str(p).strip()[:20])

def cls_art(art):
    a = str(art).lower().strip()
    if 'twistos' in a: return 'cr'
    if 'quaker' in a or 'avena' in a: return 'c'
    for mk, kws in MARCAS_KW.items():
        if any(k in a for k in kws):
            return 'pg' if mk in ['Lays','Doritos','Cheetos','3D','Pep','Tostitos'] else 'sb'
    return 'sb'

# ── VERIFICAR ARCHIVOS ─────────────────────────────────────────
print("\nVerificando archivos...")
for key, path in FILES.items():
    exists = os.path.exists(path) if path else False
    print(f"  {'OK' if exists else 'FALTA'} {key}: {os.path.basename(path) if path else 'no encontrado'}")
if not os.path.exists(FILES['template']):
    print(f"\nERROR: Falta guia_template.html en la raiz del repositorio")
    sys.exit(1)

# ── LEER CLIENTE ZONA ─────────────────────────────────────────
print("\nProcesando cliente zona...")
df_cz = pd.read_excel(FILES['cliente_zona'])
dias_map = {}
for _, row in df_cz.iterrows():
    cid = si(row['cliente_codigo'])
    zona = str(si(row.get('zona_codigo', 0)))
    vend = si(zona[:-1]) if len(zona) > 1 else 0
    dia  = si(zona[-1]) if zona else 0
    if cid > 0 and 1 <= dia <= 6 and vend > 0:
        if cid not in dias_map: dias_map[cid] = []
        dias_map[cid].append([DIA_MAP[dia], vend])

# ── LEER MAESTRO CLIENTES ─────────────────────────────────────
print("Procesando maestro clientes...")
df_mc = pd.read_excel(FILES['maestro'])
df_a = df_mc[df_mc['estado'] == 'A'].copy()
mc_dict = {}
for _, row in df_a.iterrows():
    cid = si(row['codigo'])
    mc_dict[cid] = {
        'n': clean_str(row.get('razon_social',''), 30),
        'd': clean_str(row.get('direccion',''), 35),
        'l': clean_str(row.get('localidad',''), 20),
        's': str(row.get('Segmento','')).strip()[:1],
        'v': si(row.get('vendedor', 0)),
        'm': SUP_MAP.get(si(row.get('vendedor', 0)), 0),
        'ds': dias_map.get(cid, [])[:3],
    }

# ── PROCESAR VENTAS (mes anterior = historial) ────────────────
print("Procesando ventas mes anterior...")
df_ant = pd.read_excel(FILES['venta_anterior'],
    usecols=['Cliente','Cantidad','Importe','camion','proveedor','articulo','cod_ven','tipo_venta','Razon_Social'])
df_ant = df_ant[(df_ant['camion'] < 700) &
                (df_ant['proveedor'].str.contains('Pepsico', case=False, na=False))].copy()
df_ant['art_l'] = df_ant['articulo'].str.lower().fillna('')

cli_ant = {}
for _, row in df_ant.iterrows():
    cid = si(row['Cliente'])
    v = si(row['cod_ven'])
    if v not in SUP_MAP: continue
    if cid not in cli_ant:
        cli_ant[cid] = {'rs': clean_str(row.get('Razon_Social',''), 30),
                        'v': v, 'mk': [0]*9, 'i': 0}
    art = row['art_l']
    qty = sf(row['Cantidad'])
    imp = sf(row['Importe'])
    tipo = str(row.get('tipo_venta',''))
    for idx, mk in enumerate(MARCAS):
        if any(k in art for k in MARCAS_KW[mk]):
            cli_ant[cid]['mk'][idx] += qty
            break
    if tipo == 'Venta':
        cli_ant[cid]['i'] += imp

# ── PROCESAR VENTAS (mes actual) ──────────────────────────────
print("Procesando ventas mes actual...")
df_act = pd.read_excel(FILES['venta_actual'],
    usecols=['Cliente','Cantidad','Importe','camion','proveedor','articulo','cod_ven','tipo_venta'])
df_act_pep = df_act[(df_act['camion'] < 700) &
                    (df_act['proveedor'].str.contains('Pepsico', case=False, na=False))].copy()
df_act_pep['art_l'] = df_act_pep['articulo'].str.lower().fillna('')

cli_act = {}
for _, row in df_act_pep.iterrows():
    cid = si(row['Cliente'])
    v = si(row['cod_ven'])
    if v not in SUP_MAP: continue
    if cid not in cli_act: cli_act[cid] = [0]*9
    art = row['art_l']
    qty = sf(row['Cantidad'])
    for idx, mk in enumerate(MARCAS):
        if any(k in art for k in MARCAS_KW[mk]):
            cli_act[cid][idx] += qty
            break

# ── PROCESAR OTROS PROVEEDORES ────────────────────────────────
print("Procesando otros proveedores...")
otros_prov = {}
df_act['_prov'] = df_act['proveedor'].apply(get_prov)
df_act['_v'] = df_act['cod_ven'].apply(si)
df_ant_full = pd.read_excel(FILES['venta_anterior'],
    usecols=['Cliente','Cantidad','Importe','camion','proveedor','cod_ven','tipo_venta'])
df_ant_full['_prov'] = df_ant_full['proveedor'].apply(get_prov)
df_ant_full['_v'] = df_ant_full['cod_ven'].apply(si)

for df_o, clave in [(df_ant_full,'ant'),(df_act,'act')]:
    df_o2 = df_o[df_o['camion']<700].copy()
    for _, row in df_o2.iterrows():
        v = si(row.get('cod_ven',0))
        if v not in SUP_MAP: continue
        prov = row['_prov']
        if prov in EXCLUIR_PROV: continue
        if prov not in TODOS_PROV_ACTIVOS: continue
        cid = str(si(row['Cliente']))
        qty = sf(row['Cantidad'])
        imp = sf(row['Importe'])
        tipo = str(row.get('tipo_venta',''))
        if tipo in ('Devolucion','Cambio'): qty=-abs(qty); imp=-abs(imp)
        if cid not in otros_prov: otros_prov[cid] = {}
        if prov not in otros_prov[cid]: otros_prov[cid][prov] = [0,0,0,0]
        idx_u = 0 if clave=='ant' else 2
        idx_i = 1 if clave=='ant' else 3
        otros_prov[cid][prov][idx_u] += qty
        otros_prov[cid][prov][idx_i] += imp

otros_prov_clean = {}
for cid, provs in otros_prov.items():
    entry = {}
    for prov in TODOS_PROV_ACTIVOS:
        if prov not in provs: continue
        vals = provs[prov]
        entry[prov] = [round(vals[0]),round(vals[1]),round(vals[2]),round(vals[3])]
    if entry: otros_prov_clean[cid] = entry

# ── CONSTRUIR GUIA_DATA ───────────────────────────────────────
print("Construyendo GUIA_DATA...")
guia = {}
for cid, d in cli_ant.items():
    mc = mc_dict.get(cid, {})
    vend = mc.get('v') or d['v']
    mesa = SUP_MAP.get(vend, 0)
    if mesa == 0: continue
    uds = [max(0, round(x)) for x in d['mk']]
    guia[str(cid)] = {
        'n': mc.get('n') or d['rs'],
        'l': mc.get('l',''), 'd': mc.get('d',''),
        's': mc.get('s',''), 'v': vend, 'm': mesa,
        'ds': mc.get('ds',[]),
        'i': round(d['i']),
        'u': uds,
    }

abr_data = {str(k): [max(0,round(x)) for x in v] for k,v in cli_act.items()}

# ── STATS POR VENDEDOR ────────────────────────────────────────
CARTERA_CZ = {31:274,32:286,33:247,34:263,35:275,36:254,37:272,38:304,39:299,
    41:305,42:282,43:280,44:315,45:337,46:256,47:297,48:313,49:317,
    51:245,52:332,53:303,54:360,55:315,56:292,57:274,58:297,59:298}

vend_stats = {}
for cid, d in guia.items():
    v = d.get('v',0)
    if v not in CARTERA_CZ: continue
    if v not in vend_stats:
        vend_stats[v] = {'ccc_m':0,'cob9':0,'mk_sum':0,'imp_m':0,'ccc_a':0}
    s = vend_stats[v]
    s['ccc_m'] += 1
    n_ok = sum(1 for u in d['u'] if u >= 3)
    s['cob9'] += (1 if n_ok == 9 else 0)
    s['mk_sum'] += n_ok
    s['imp_m'] += d.get('i',0)

for cid, uds in abr_data.items():
    if cid not in guia: continue
    v = guia[cid].get('v',0)
    if v not in vend_stats: continue
    if any(u >= 1 for u in uds): vend_stats[v]['ccc_a'] += 1

vend_stats_out = {}
for v, s in vend_stats.items():
    cart = CARTERA_CZ.get(v, 0)
    vend_stats_out[str(v)] = {
        'cart': cart,
        'ccc_m': s['ccc_m'], 'pcc_m': round(s['ccc_m']/cart*100,1) if cart else 0,
        'ccc_a': s['ccc_a'], 'pcc_a': round(s['ccc_a']/cart*100,1) if cart else 0,
        'cob9': s['cob9'],
        'cob_p': round(s['mk_sum']/s['ccc_m'],1) if s['ccc_m'] else 0,
        'imp_m': round(s['imp_m']),
    }

# ── SERIALIZAR A JSON ASCII-SAFE ──────────────────────────────
print("Serializando datos...")
guia_js  = 'const GUIA_DATA='  + json.dumps(guia, ensure_ascii=True, separators=(',',':')) + ';'
abr_js   = 'const ABR_DATA='   + json.dumps(abr_data, ensure_ascii=True, separators=(',',':')) + ';'
otros_js = 'const OTROS_PROV_DET=' + json.dumps(otros_prov_clean, ensure_ascii=True, separators=(',',':')) + ';\n'
otros_js += 'const TODOS_PROV_ACTIVOS=' + json.dumps(TODOS_PROV_ACTIVOS, ensure_ascii=True, separators=(',',':')) + ';'
stats_js = 'const VEND_STATS=' + json.dumps(vend_stats_out, ensure_ascii=True, separators=(',',':')) + ';'

# ── LEER TEMPLATE E INYECTAR DATOS ───────────────────────────
print("Generando HTML final...")
with open(FILES['template'], 'r', encoding='utf-8') as f:
    html = f.read()

html = html.replace('// __GUIA_DATA__',   guia_js)
html = html.replace('// __ABR_DATA__',    abr_js)
html = html.replace('// __OTROS_PROV__',  otros_js)
html = html.replace('// __VEND_STATS__',  stats_js)

# Agregar fecha de generacion
fecha = datetime.now().strftime('%d/%m/%Y %H:%M')
html = html.replace('__FECHA_GENERACION__', fecha)

with open(FILES['output'], 'w', encoding='utf-8') as f:
    f.write(html)

size = os.path.getsize(FILES['output']) / 1024
print(f"\nGenerado correctamente: {FILES['output']}")
print(f"Tamano: {size:.0f} KB")
print(f"Clientes: {len(guia)}")
print(f"Fecha: {fecha}")
print("\nListo para distribuir a los vendedores!")
