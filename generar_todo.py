"""
generar_todo.py
---------------
Script maestro que genera EN PARALELO:
  - guia_visita_611.html   (para vendedores)
  - mas_analytics_v9.html  (para supervisores)

Archivos requeridos en data/:
  - venta_actual.xlsx        (sabana mes en curso - se pisa cada dia)
  - venta_anterior.xlsx      (sabana mes cerrado - se renueva mensualmente)
  - venta_enero.xlsx etc     (historico - opcional, uno por mes)
  - maestro_clientes.xlsx
  - cliente_zona.xlsx
  - objetivos_MESNAME.xlsx   (ej: objetivos_abril.xlsx, objetivos_mayo.xlsx)

Genera en la raiz del repo:
  - guia_visita_611.html
  - mas_analytics_v9.html
"""

import pandas as pd
import json, re, os, sys, math
from datetime import datetime
from collections import defaultdict

print("=" * 60)
print("MAS Analytics 611 - Generador Maestro")
print("=" * 60)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# ─── CONSTANTES ───────────────────────────────────────────────

VEND_NOMBRES = {
    31:'VILLALBA MAX.',32:'OLIVARES EDGAR',33:'ORDOÑEZ ISMAEL',
    34:'BALDO JULIAN',35:'BAZAN LUCIANO',36:'NEIRA NAHUEL',
    37:'SCORRANI IGN.',38:'MARTINEZ MAU.',39:'MERLO SEBASTIAN',
    41:'CISTERNA DIEGO',42:'RESTIVO VALERIA',43:'RUEDA CUJAR',
    44:'BERNAHOLA AGU.',45:'ROMERO DIANA',46:'OCHOA ROXANA',
    47:'CACERES YANINA',48:'SALDE MARCELA',49:'MOREIRA TOBIAS',
    51:'GONZALEZ CAND.',52:'MARTINEZ DIEGO',53:'CARRIZO EZE.',
    54:'MORENO JOHAN',55:'PULIDO EFRAIM',56:'RIVARA LORENZO',
    57:'DIAZ OMAR',58:'GONZALEZ MARC.',59:'AVENDAÑO SERGIO',
    61:'PACHECO RODRIGO',62:'PAEZ SEBASTIAN',63:'MIRANDA MARIANA',
    64:'GONZALEZ FRANCO',65:'SANCHEZ JULIAN'
}

SUP_MAP = {}
for v in [41,45,49,52,53,55,56,57,59]: SUP_MAP[v]={'sup':'NATALIA PEREZ','mesa':300}
for v in [31,32,33,34,35,36,37,38,39,51]: SUP_MAP[v]={'sup':'CLAUDIO ALVARADO','mesa':400}
for v in [42,43,44,46,47,48,54,58]: SUP_MAP[v]={'sup':'MARIANO TRIULCI','mesa':500}
for v in [61,62,63,64,65]: SUP_MAP[v]={'sup':'NATALIA PEREZ','mesa':600}

MARCAS = ['Lays','Doritos','Cheetos','3D','Pep','Pehuamar','Twistos','Tostitos','Quaker']
MARCAS_KW = {
    'Lays':['lays'],'Doritos':['doritos'],'Cheetos':['cheetos'],'3D':['3d'],
    'Pep':['pep comun','pep rueditas','pep ramitas','pep '],
    'Pehuamar':['pehuamar'],'Twistos':['twistos'],
    'Tostitos':['tostitos'],'Quaker':['quaker','avena']
}
DIA_MAP = {1:'Lunes',2:'Martes',3:'Miercoles',4:'Jueves',5:'Viernes',6:'Sabado'}

PROV_MAP = {
    'Pepsico de Argentina SRL':'PepsiCo',
    'MOLINOS RIO DE LA PLATA SA':'Molinos',
    'SOFTYS ARGENTINA SA':'Softys',
    'GEORGALOS HNOS S A I C A':'Georgalos',
    'DON SATUR SRL':'Don Satur',
    'GENOMMA LABORATORIES ARGENTINA SA':'Genomma',
    'INDUSTRIAS QUIMICAS Y MINERAS TIMBO SA':'Timbo',
    'BUHL SA':'Buhl',
    'TRES H BEBIDAS S.A.S.':'Tres H',
    'DULCOR SA':'Dulcor',
    'JOSE LLENES SACIF':'Llenes',
    'CRONI SA':'Croni',
    'COOPERATIVA AGRICOLA DE LA COLONIA LIEBIG LTDA':'Liebig',
}
EXCLUIR_PROV = {'PepsiCo','Molinos','Softys','Combos seisonce','Administracion 611'}
OTROS_PROV_ACTIVOS = ['Don Satur','Georgalos','Genomma','Timbo','Buhl',
                      'Tres H','Dulcor','Liebig','Llenes','Croni']

MESES_ES = {1:'Ene',2:'Feb',3:'Mar',4:'Abr',5:'May',6:'Jun',
            7:'Jul',8:'Ago',9:'Sep',10:'Oct',11:'Nov',12:'Dic'}

# ─── UTILS ────────────────────────────────────────────────────

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

def classify_art(art):
    a = str(art).lower().strip()
    if 'twistos' in a: return 'crackers'
    if 'quaker' in a or 'avena' in a: return 'cereales'
    for mk, kws in MARCAS_KW.items():
        if any(k in a for k in kws):
            if mk in ['Lays','Doritos','Cheetos','3D','Pep','Tostitos']: return 'salty_pg'
            if mk == 'Pehuamar': return 'salty_sb'
    if any(x in a for x in ['lays','doritos','cheetos','3d ','tostitos','pep ','dinamita','mani','maniax']):
        return 'salty_pg'
    return 'salty_sb'

# Detectar si PG o SB por lista de precios
ART_CAT_CACHE = {}
def classify_art_full(art, desc_cat={}):
    a = str(art).lower().strip()
    if a in ART_CAT_CACHE: return ART_CAT_CACHE[a]
    if 'twistos' in a: r='crackers'
    elif 'quaker' in a or 'avena' in a: r='cereales'
    else:
        found = None
        for k, cat in desc_cat.items():
            if k and len(k)>5 and k in a:
                found = cat; break
        if found in ('Platinum','Gold'): r='salty_pg'
        elif found in ('Silver','Bronze'): r='salty_sb'
        elif any(x in a for x in ['lays','doritos','cheetos','3d ','3d q','tostitos','pep ','pep r','pep c','dinamita','mani','maniax']): r='salty_pg'
        else: r='salty_sb'
    ART_CAT_CACHE[a] = r
    return r

def find_file(name, fuzzy_keywords=None):
    """Busca archivo por nombre exacto (case-insensitive) o por keywords."""
    name_norm = name.lower().replace('_',' ')
    for f in os.listdir(DATA_DIR):
        if not f.endswith('.xlsx'): continue
        f_norm = f.lower().replace('_',' ')
        if f_norm == name_norm or f.lower() == name.lower():
            return os.path.join(DATA_DIR, f)
    if fuzzy_keywords:
        for f in sorted(os.listdir(DATA_DIR)):
            if not f.endswith('.xlsx'): continue
            fl = f.lower()
            if all(k in fl for k in fuzzy_keywords):
                return os.path.join(DATA_DIR, f)
    return None

def find_all_ventas():
    """Encuentra todos los archivos de venta (actual, anterior, historicos)."""
    excluir = ['maestro','zona','objetivo','template','guia','cliente']
    ventas = {}
    for f in sorted(os.listdir(DATA_DIR)):
        if not f.endswith('.xlsx') or f.startswith('~'): continue
        fl = f.lower()
        if any(k in fl for k in excluir): continue
        path = os.path.join(DATA_DIR, f)
        ventas[f] = path
    return ventas

def find_all_objetivos():
    """
    Encuentra archivos de objetivos por mes. Soporta dos formatos:
      objetivos_kg_abril.xlsx  + objetivos_ccc_abril.xlsx  (dos archivos separados)
      objetivos_abril.xlsx                                  (un solo archivo combinado)
    Retorna: {mes_num: {'kg': path_o_None, 'ccc': path_o_None}}
    """
    MESES = [('enero',1),('febrero',2),('marzo',3),('abril',4),
             ('mayo',5),('junio',6),('julio',7),('agosto',8),
             ('septiembre',9),('octubre',10),('noviembre',11),('diciembre',12)]
    objetivos = {}
    for f in sorted(os.listdir(DATA_DIR)):
        if not f.endswith('.xlsx'): continue
        fl = f.lower()
        if 'objetivo' not in fl and 'obj_' not in fl: continue
        path = os.path.join(DATA_DIR, f)
        for mes_n, mes_num in MESES:
            if mes_n not in fl: continue
            if mes_num not in objetivos:
                objetivos[mes_num] = {'kg': None, 'ccc': None}
            if 'ccc' in fl:
                objetivos[mes_num]['ccc'] = path
                print(f"  Objetivo CCC: {f} -> mes {mes_num}")
            elif 'kg' in fl:
                objetivos[mes_num]['kg'] = path
                print(f"  Objetivo KG:  {f} -> mes {mes_num}")
            else:
                objetivos[mes_num]['kg'] = path
                print(f"  Objetivo:     {f} -> mes {mes_num}")
            break
    return objetivos

def leer_objetivos_mes(path):
    """Lee archivo de objetivos (PG+SB) y retorna dict {vend: {pg, sb, total}}"""
    obj = {}
    try:
        df = pd.read_excel(path)
        for _, row in df.iterrows():
            v = si(row.get('ven_id', row.get('CODIGO', row.get('codigo', 0))))
            if v <= 0: continue
            cat = str(row.get('SubGrupoCupo_Descripcion', row.get('categoria', ''))).lower()
            val = sf(row.get('objetivo', row.get('OBJETIVO', 0)))
            if v not in obj: obj[v] = {'pg':0,'sb':0,'c':0,'cr':0}
            if 'platin' in cat or 'gold' in cat: obj[v]['pg'] = round(val)
            elif 'silver' in cat or 'bronze' in cat: obj[v]['sb'] = round(val)
            elif 'cereal' in cat or 'quaker' in cat: obj[v]['c'] = round(val)
            elif 'crack' in cat or 'twistos' in cat: obj[v]['cr'] = round(val)
    except Exception as e:
        print(f"  Error leyendo objetivos: {e}")
    for v in obj:
        obj[v]['total'] = sum(obj[v].values())
    return obj

def leer_objetivos_ccc(path):
    """Lee objetivos CCC retorna {vend: {obj_ccc, universo}}"""
    obj = {}
    try:
        df = pd.read_excel(path)
        for _, row in df.iterrows():
            try:
                v = si(row.get('CODIGO', row.get('codigo',0)))
                if v <= 0: continue
                obj[v] = {
                    'obj_ccc': round(sf(row.get('OBJETIVO', row.get('objetivo',0)))),
                    'universo': si(row.get('UNIVERSO', row.get('universo',0)))
                }
            except: pass
    except: pass
    return obj

# Objetivos base de marzo (fallback si no hay archivo para el mes)
OBJ_BASE = {
    41:{'pg':731,'sb':321,'c':15,'cr':99},
    45:{'pg':672,'sb':330,'c':23,'cr':128},
    49:{'pg':755,'sb':264,'c':22,'cr':125},
    52:{'pg':883,'sb':375,'c':33,'cr':147},
    53:{'pg':892,'sb':383,'c':26,'cr':150},
    55:{'pg':668,'sb':376,'c':22,'cr':111},
    56:{'pg':1247,'sb':608,'c':71,'cr':170},
    57:{'pg':748,'sb':268,'c':16,'cr':121},
    59:{'pg':847,'sb':546,'c':76,'cr':131},
    31:{'pg':1578,'sb':596,'c':51,'cr':196},
    32:{'pg':855,'sb':525,'c':18,'cr':149},
    33:{'pg':1178,'sb':400,'c':39,'cr':170},
    34:{'pg':1066,'sb':366,'c':14,'cr':182},
    35:{'pg':1029,'sb':322,'c':48,'cr':149},
    36:{'pg':1803,'sb':649,'c':19,'cr':268},
    37:{'pg':1000,'sb':425,'c':14,'cr':189},
    38:{'pg':1453,'sb':700,'c':119,'cr':189},
    39:{'pg':1290,'sb':670,'c':20,'cr':263},
    51:{'pg':1024,'sb':587,'c':54,'cr':181},
    42:{'pg':802,'sb':432,'c':18,'cr':123},
    43:{'pg':815,'sb':338,'c':45,'cr':130},
    44:{'pg':748,'sb':341,'c':25,'cr':108},
    46:{'pg':810,'sb':232,'c':38,'cr':120},
    47:{'pg':835,'sb':239,'c':11,'cr':114},
    48:{'pg':1697,'sb':680,'c':49,'cr':206},
    54:{'pg':857,'sb':359,'c':13,'cr':122},
    58:{'pg':981,'sb':281,'c':14,'cr':170},
}
for v in OBJ_BASE:
    OBJ_BASE[v]['total'] = sum(OBJ_BASE[v].values())

OBJ_ABR_PG_SB = {
    31:{'pg':2080,'sb':710},32:{'pg':1029,'sb':555},33:{'pg':1315,'sb':521},
    34:{'pg':1031,'sb':319},35:{'pg':1172,'sb':358},36:{'pg':1401,'sb':615},
    37:{'pg':1145,'sb':385},38:{'pg':1099,'sb':611},39:{'pg':1263,'sb':537},
    51:{'pg':1208,'sb':646},41:{'pg':894,'sb':331},45:{'pg':1110,'sb':378},
    49:{'pg':806,'sb':257},52:{'pg':1067,'sb':446},53:{'pg':937,'sb':363},
    55:{'pg':1011,'sb':514},56:{'pg':1236,'sb':677},57:{'pg':879,'sb':284},
    59:{'pg':871,'sb':442},42:{'pg':1077,'sb':523},43:{'pg':877,'sb':355},
    44:{'pg':952,'sb':407},46:{'pg':1053,'sb':306},47:{'pg':956,'sb':251},
    48:{'pg':2094,'sb':802},54:{'pg':1040,'sb':395},58:{'pg':1284,'sb':329},
}

OBJ_CCC_ABR = {
    31:{'obj_ccc':214,'universo':238},32:{'obj_ccc':201,'universo':223},
    33:{'obj_ccc':215,'universo':239},34:{'obj_ccc':202,'universo':224},
    35:{'obj_ccc':222,'universo':261},36:{'obj_ccc':205,'universo':228},
    37:{'obj_ccc':202,'universo':208},38:{'obj_ccc':224,'universo':287},
    39:{'obj_ccc':222,'universo':261},51:{'obj_ccc':193,'universo':195},
    41:{'obj_ccc':210,'universo':300},45:{'obj_ccc':220,'universo':329},
    49:{'obj_ccc':208,'universo':306},52:{'obj_ccc':213,'universo':313},
    53:{'obj_ccc':215,'universo':295},55:{'obj_ccc':211,'universo':281},
    56:{'obj_ccc':213,'universo':266},57:{'obj_ccc':211,'universo':240},
    59:{'obj_ccc':210,'universo':269},42:{'obj_ccc':213,'universo':277},
    43:{'obj_ccc':211,'universo':274},44:{'obj_ccc':215,'universo':312},
    46:{'obj_ccc':213,'universo':251},47:{'obj_ccc':215,'universo':279},
    48:{'obj_ccc':216,'universo':308},54:{'obj_ccc':213,'universo':304},
    58:{'obj_ccc':212,'universo':259},
}

def get_obj_for_mes(mes_num):
    """Retorna objetivos para un mes dado. Lee archivo si existe, sino usa base."""
    obj_paths = objetivos_files.get(mes_num, {})
    obj_path = obj_paths.get('kg') if isinstance(obj_paths, dict) else obj_paths
    if obj_path:
        obj = leer_objetivos_mes(obj_path)
        if obj:
            # Completar con base para los que falten
            for v in OBJ_BASE:
                if v not in obj:
                    obj[v] = dict(OBJ_BASE[v])
                else:
                    # Si faltan c/cr usar los de base
                    for cat in ['c','cr']:
                        if not obj[v].get(cat):
                            obj[v][cat] = OBJ_BASE[v].get(cat,0)
                    obj[v]['total'] = obj[v]['pg']+obj[v]['sb']+obj[v].get('c',0)+obj[v].get('cr',0)
            return obj
    # Sin archivo: abril usa OBJ_ABR_PG_SB, otros usan base
    if mes_num == 4:
        obj = {}
        for v, d in OBJ_ABR_PG_SB.items():
            base = OBJ_BASE.get(v, {})
            obj[v] = {
                'pg': d['pg'], 'sb': d['sb'],
                'c': base.get('c',0), 'cr': base.get('cr',0),
                'total': d['pg']+d['sb']+base.get('c',0)+base.get('cr',0)
            }
        return obj
    return {v: dict(d) for v, d in OBJ_BASE.items()}

def get_ccc_obj_for_mes(mes_num):
    # Primero buscar archivo
    obj_paths = objetivos_files.get(mes_num, {})
    ccc_path = obj_paths.get('ccc') if isinstance(obj_paths, dict) else None
    if ccc_path:
        return leer_objetivos_ccc(ccc_path)
    # Fallback: abril usa hardcoded
    if mes_num == 4:
        return OBJ_CCC_ABR
    return {}

# ─── DETERMINAR MES DE UN ARCHIVO ─────────────────────────────

def detect_mes(filepath):
    """Lee algunas filas para determinar el mes del archivo."""
    try:
        df = pd.read_excel(filepath, usecols=['fecha'], nrows=100)
        fechas = pd.to_datetime(df['fecha'], errors='coerce').dropna()
        if len(fechas) > 0:
            mes = fechas.mode()[0].month
            anio = fechas.mode()[0].year
            return mes, anio
    except:
        pass
    # Intentar detectar por nombre del archivo
    fname = os.path.basename(filepath).lower()
    for mes_n, mes_num in [
        ('enero',1),('febrero',2),('marzo',3),('abril',4),
        ('mayo',5),('junio',6),('julio',7),('agosto',8),
        ('sept',9),('octu',10),('novie',11),('dicie',12)
    ]:
        if mes_n in fname:
            return mes_num, 2026
    return None, None

# ─── PROCESAR UN ARCHIVO DE VENTAS ────────────────────────────

def procesar_ventas(filepath, obj_vend, cartera_cz):
    """Procesa una sábana y retorna datos de performance."""
    print(f"  Procesando: {os.path.basename(filepath)}...")

    df = pd.read_excel(filepath,
        usecols=['Cliente','Cantidad','Importe','camion','proveedor',
                 'articulo','cod_ven','tipo_venta','Razon_Social'])
    df['_v']   = df['cod_ven'].apply(si)
    df['_pep'] = df['proveedor'].str.contains('Pepsico',case=False,na=False)
    df['_crea']= df['camion'] >= 700
    df['_kg']  = df['articulo'].apply(exkg) * df['Cantidad'].abs()
    df['_cat'] = df['articulo'].apply(lambda a: classify_art_full(a, desc_cat))
    df['_prov']= df['proveedor'].apply(get_prov)

    df_real = df[~df['_crea']].copy()
    pep_real = df_real[df_real['_pep']]

    # CCC PepsiCo (mesas 300/400/500)
    neto_uds = pep_real.groupby('Cliente')['Cantidad'].sum()
    neto_imp  = pep_real.groupby('Cliente')['Importe'].sum()
    ccc_pep_set = set(neto_uds[(neto_uds>0)&(neto_imp>0)].index)

    # CCC mesa 600
    ccc_600 = {}
    for v in [61,62,63,64,65]:
        dv = df_real[df_real['_v']==v]
        if dv.empty: continue
        cli_set = set()
        for prov in dv['proveedor'].unique():
            dp = dv[dv['proveedor']==prov]
            nu = dp.groupby('Cliente')['Cantidad'].sum()
            ni = dp.groupby('Cliente')['Importe'].sum()
            cli_set |= set(nu[(nu>0)&(ni>0)].index)
        ccc_600[v] = cli_set

    # Acumular por vendedor
    vend_acc = {}
    for _, row in df.iterrows():
        v = si(row['_v'])
        if v not in VEND_NOMBRES: continue
        imp  = sf(row['Importe'])
        kg   = sf(row['_kg'])
        cat  = row['_cat']
        cli  = si(row['Cliente'])
        tipo = str(row.get('tipo_venta',''))
        pep  = bool(row['_pep'])
        crea = bool(row['_crea'])
        mesa = SUP_MAP.get(v,{}).get('mesa',0)

        if v not in vend_acc:
            vend_acc[v] = {
                'pep_v':0,'pep_d':0,'pep_c':0,'pep_kg':0,'pep_crea_kg':0,
                'cat_r':{'cereales':0,'crackers':0,'salty_pg':0,'salty_sb':0},
                'tot_v':0,'tot_d':0,'tot_c':0,
                'ccc_pep':set(),'ccc_tot':set(),
                'rs': clean_str(row.get('Razon_Social',''), 30),
            }
        d = vend_acc[v]

        if not crea:
            if tipo=='Venta': d['tot_v']+=imp
            elif tipo=='Devolucion': d['tot_d']+=abs(imp)
            elif tipo=='Cambio': d['tot_c']+=abs(imp)

        if pep:
            if not crea:
                if tipo=='Venta':
                    d['pep_v']+=imp; d['pep_kg']+=kg
                    if cli in ccc_pep_set: d['ccc_pep'].add(cli)
                    d['cat_r'][cat]+=kg
                elif tipo=='Devolucion': d['pep_d']+=abs(imp)
                elif tipo=='Cambio': d['pep_c']+=abs(imp)
            else:
                d['pep_crea_kg']+=kg
        if mesa==600:
            d['ccc_tot'] = ccc_600.get(v,set())

    # Construir lista final
    perf = []
    for v, d in vend_acc.items():
        if v not in SUP_MAP: continue
        obj  = obj_vend.get(v,{})
        mesa = SUP_MAP[v]['mesa']
        kr   = round(d['pep_kg'])
        kc   = round(d['pep_crea_kg'])
        kt   = kr + kc
        ot   = obj.get('total',0)
        apr  = round(kr/ot*100,1) if ot else None
        pv   = d['pep_v']
        tv   = d['tot_v']
        ccc_n= len(d['ccc_pep']) if mesa in [300,400,500] else len(d['ccc_tot'])
        cart = cartera_cz.get(v, 0)

        cat_data = {}
        for cat in ['cereales','crackers','salty_pg','salty_sb']:
            oc = obj.get(cat,0) if cat!='salty_pg' else obj.get('pg',0)
            if cat=='salty_sb': oc=obj.get('sb',0)
            rc = round(d['cat_r'].get(cat,0))
            cat_data[cat] = {
                'o':oc,'r':rc,
                'pr':round(rc/oc*100,1) if oc else None,
                'g':oc-rc
            }

        perf.append({
            'cod':v,'nom':VEND_NOMBRES[v],
            'mesa':mesa,'sup':SUP_MAP[v]['sup'],
            'cart':cart,'ccc':ccc_n,
            'pcc':round(ccc_n/cart*100,1) if cart else 0,
            'kr':kr,'kc':kc,'kt':kt,'ot':ot,'apr':apr,
            'pv':round(pv),'pd':round(d['pep_d']),'pc':round(d['pep_c']),
            'pdp':round(d['pep_d']/pv*100,2) if pv else 0,
            'tv':round(tv),'td':round(d['tot_d']),'tc':round(d['tot_c']),
            'tdp':round(d['tot_d']/tv*100,2) if tv else 0,
            'cat':cat_data,
        })

    perf.sort(key=lambda x: x['mesa']*1000+x['cod'])

    mesas_data = {}
    for mesa in [300,400,500,600]:
        pvs = [p for p in perf if p['mesa']==mesa]
        if not pvs: continue
        tot_kg  = sum(p['kr'] for p in pvs)
        tot_obj = sum(p['ot'] for p in pvs)
        tot_tv  = sum(p['tv'] for p in pvs)
        tot_td  = sum(p['td'] for p in pvs)
        tot_ccc = sum(p['ccc'] for p in pvs)
        tot_cart= sum(p['cart'] for p in pvs)
        mesas_data[str(mesa)] = {
            'kg_real':tot_kg,'obj_total':tot_obj,
            'avance_pct':round(tot_kg/tot_obj*100,1) if tot_obj else 0,
            'imp_real':tot_tv,'devol':tot_td,
            'devol_pct':round(tot_td/tot_tv*100,2) if tot_tv else 0,
            'ccc':tot_ccc,'cartera':tot_cart,
            'pct_ccc':round(tot_ccc/tot_cart*100,1) if tot_cart else 0,
        }

    return {'perf':perf,'mesas':mesas_data}

# ─── PROCESAR TODOS LOS ARCHIVOS ──────────────────────────────

print("\nProcesando archivos de venta...")

# Identificar archivo actual y anterior
venta_actual_path   = find_file("venta_actual.xlsx")
venta_anterior_path = find_file("venta_anterior.xlsx")

# Si no hay con nombre fijo, tomar los mas recientes del listado
todos_ventas = find_all_ventas()
if not venta_actual_path and todos_ventas:
    # El mas reciente es el actual
    recientes = sorted(todos_ventas.items(),
                       key=lambda x: os.path.getmtime(x[1]), reverse=True)
    venta_actual_path = recientes[0][1]
    if len(recientes) > 1:
        venta_anterior_path = recientes[1][1]
    print(f"  Actual (auto): {os.path.basename(venta_actual_path)}")
    if venta_anterior_path:
        print(f"  Anterior (auto): {os.path.basename(venta_anterior_path)}")

if not venta_actual_path:
    print("ERROR: No se encontro venta_actual.xlsx")
    sys.exit(1)

# Detectar mes del archivo actual
mes_actual, anio_actual = detect_mes(venta_actual_path)
if not mes_actual:
    mes_actual = datetime.now().month
    anio_actual = datetime.now().year
periodo_actual = f"{MESES_ES[mes_actual]} {anio_actual}"
print(f"  Mes actual: {periodo_actual}")

# Objetivos del mes actual
obj_actual = get_obj_for_mes(mes_actual)
ccc_obj_actual = get_ccc_obj_for_mes(mes_actual)

# Agregar obj_ccc a cada vendedor
for v, d in obj_actual.items():
    ccc_info = ccc_obj_actual.get(v,{})
    d['obj_ccc'] = ccc_info.get('obj_ccc',0)
    d['universo'] = ccc_info.get('universo', cartera_cz.get(v,0))

# Procesar mes actual
print(f"\nProcesando mes actual ({periodo_actual})...")
data_actual = procesar_ventas(venta_actual_path, obj_actual, cartera_cz)

# Procesar mes anterior (para comparativas)
data_anterior = None
periodo_anterior = None
if venta_anterior_path and venta_anterior_path != venta_actual_path:
    mes_ant, anio_ant = detect_mes(venta_anterior_path)
    if mes_ant:
        periodo_anterior = f"{MESES_ES[mes_ant]} {anio_ant}"
        obj_anterior = get_obj_for_mes(mes_ant)
        print(f"\nProcesando mes anterior ({periodo_anterior})...")
        data_anterior = procesar_ventas(venta_anterior_path, obj_anterior, cartera_cz)

# Procesar historico (todos los archivos con venta_mes*.xlsx o similares)
PERF_DATA = {}
if data_anterior and periodo_anterior:
    PERF_DATA[periodo_anterior] = {
        'periodo':periodo_anterior,
        'perf': data_anterior['perf'],
        'mesas': data_anterior['mesas']
    }
PERF_DATA[periodo_actual] = {
    'periodo':periodo_actual,
    'perf': data_actual['perf'],
    'mesas': data_actual['mesas']
}

# Tambien procesar archivos con nombre de mes en el nombre
for fname, fpath in todos_ventas.items():
    if fpath in [venta_actual_path, venta_anterior_path]: continue
    mes_h, anio_h = detect_mes(fpath)
    if not mes_h: continue
    periodo_h = f"{MESES_ES[mes_h]} {anio_h}"
    if periodo_h in PERF_DATA: continue
    print(f"\nProcesando historico ({periodo_h})...")
    obj_h = get_obj_for_mes(mes_h)
    data_h = procesar_ventas(fpath, obj_h, cartera_cz)
    PERF_DATA[periodo_h] = {
        'periodo':periodo_h,
        'perf': data_h['perf'],
        'mesas': data_h['mesas']
    }

print(f"\nPeriodos procesados: {list(PERF_DATA.keys())}")

# ─── DATOS PARA GUIA DE VISITA ─────────────────────────────────

print("\nGenerando datos para guia de visita...")

# Mes anterior = historial cobertura marcas
df_ant_full = pd.read_excel(venta_anterior_path or venta_actual_path,
    usecols=['Cliente','Cantidad','Importe','camion','proveedor',
             'articulo','cod_ven','tipo_venta','Razon_Social'])
df_ant_full = df_ant_full[
    (df_ant_full['camion']<700) &
    df_ant_full['proveedor'].str.contains('Pepsico',case=False,na=False)
].copy()
df_ant_full['art_l'] = df_ant_full['articulo'].str.lower().fillna('')

cli_ant = {}
for _, row in df_ant_full.iterrows():
    cid = si(row['Cliente'])
    v   = si(row['cod_ven'])
    if v not in SUP_MAP: continue
    if cid not in cli_ant:
        cli_ant[cid] = {'v':v,'mk':[0]*9,'i':0,
                        'rs':clean_str(row.get('Razon_Social',''),30)}
    art = row['art_l']
    qty = sf(row['Cantidad'])
    for idx, mk in enumerate(MARCAS):
        if any(k in art for k in MARCAS_KW[mk]):
            cli_ant[cid]['mk'][idx] += qty
            break
    if str(row.get('tipo_venta',''))=='Venta':
        cli_ant[cid]['i'] += sf(row['Importe'])

# Mes actual (para ABR_DATA)
df_act_guia = pd.read_excel(venta_actual_path,
    usecols=['Cliente','Cantidad','camion','proveedor','articulo','cod_ven','tipo_venta'])
df_act_guia = df_act_guia[
    (df_act_guia['camion']<700) &
    df_act_guia['proveedor'].str.contains('Pepsico',case=False,na=False)
].copy()
df_act_guia['art_l'] = df_act_guia['articulo'].str.lower().fillna('')

cli_act = {}
for _, row in df_act_guia.iterrows():
    cid = si(row['Cliente'])
    v   = si(row['cod_ven'])
    if v not in SUP_MAP: continue
    if cid not in cli_act: cli_act[cid] = [0]*9
    art = row['art_l']
    qty = sf(row['Cantidad'])
    for idx, mk in enumerate(MARCAS):
        if any(k in art for k in MARCAS_KW[mk]):
            cli_act[cid][idx] += qty
            break

# Otros proveedores
print("Procesando otros proveedores para guia...")
otros_prov = {}
df_act_todos = pd.read_excel(venta_actual_path,
    usecols=['Cliente','Cantidad','Importe','camion','proveedor','cod_ven','tipo_venta'])
df_act_todos['_prov'] = df_act_todos['proveedor'].apply(get_prov)

df_ant_todos = pd.read_excel(venta_anterior_path or venta_actual_path,
    usecols=['Cliente','Cantidad','Importe','camion','proveedor','cod_ven','tipo_venta'])
df_ant_todos['_prov'] = df_ant_todos['proveedor'].apply(get_prov)

for df_o, clave in [(df_ant_todos,'ant'),(df_act_todos,'act')]:
    df_o2 = df_o[df_o['camion']<700].copy()
    for _, row in df_o2.iterrows():
        v = si(row.get('cod_ven',0))
        if v not in SUP_MAP: continue
        prov = row['_prov']
        if prov in EXCLUIR_PROV or prov not in OTROS_PROV_ACTIVOS: continue
        cid  = str(si(row['Cliente']))
        qty  = sf(row['Cantidad'])
        imp  = sf(row['Importe'])
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
    entry = {p:[round(v) for v in vals] for p,vals in provs.items() if p in OTROS_PROV_ACTIVOS}
    if entry: otros_prov_clean[cid] = entry

# Construir GUIA_DATA
guia = {}
for cid, d in cli_ant.items():
    mc   = mc_dict.get(cid,{})
    vend = mc.get('v') or d['v']
    mesa = SUP_MAP.get(vend,{}).get('mesa',0)
    if mesa == 0: continue
    uds  = [max(0,round(x)) for x in d['mk']]
    guia[str(cid)] = {
        'n': mc.get('n') or d['rs'],
        'l': mc.get('l',''), 'd': mc.get('d',''),
        'v': vend, 'm': mesa,
        'ds': mc.get('ds',[]),
        'i': round(d['i']),
        'u': uds,
    }

abr_data = {str(k):[max(0,round(x)) for x in v] for k,v in cli_act.items()}

# VEND_STATS para guia
MK_TARGETS = {'Lays':94.9,'Doritos':98.0,'Cheetos':83.9,'3D':85.7,
    'Pep':62.3,'Pehuamar':20.7,'Twistos':79.4,'Tostitos':28.4,'Quaker':4.6}

# Cobertura mes actual por vendedor por marca
# Usar mc_dict para obtener el vendedor (cubre clientes nuevos que no estan en guia)
cob_abr = {}
for cid_int, uds in cli_act.items():
    cid_str = str(cid_int)
    # Buscar vendedor: primero en guia, luego en mc_dict
    d = guia.get(cid_str, {})
    v = d.get('v', 0)
    if not v:
        mc = mc_dict.get(int(cid_int) if isinstance(cid_int,str) else cid_int, {})
        v = mc.get('v', 0)
    if v not in SUP_MAP: continue
    if v not in cob_abr: cob_abr[v] = {mk:0 for mk in MARCAS}
    for mk_idx, mk in enumerate(MARCAS):
        if len(uds) > mk_idx and uds[mk_idx] >= 3:
            cob_abr[v][mk] += 1

vend_stats = {}
for cid, d in guia.items():
    v = d.get('v',0)
    if v not in cartera_cz: continue
    if v not in vend_stats:
        vend_stats[v] = {'ccc_m':0,'cob9':0,'mk_sum':0,'imp_m':0,'ccc_a':0}
    s = vend_stats[v]
    s['ccc_m'] += 1
    n_ok = sum(1 for u in d['u'] if u >= 3)
    if n_ok == 9: s['cob9'] += 1
    s['mk_sum'] += n_ok
    s['imp_m']  += d.get('i',0)

for cid, uds in abr_data.items():
    if cid not in guia: continue
    v = guia[cid].get('v',0)
    if v not in vend_stats: continue
    if any(u >= 1 for u in uds): vend_stats[v]['ccc_a'] += 1

vend_stats_out = {}
for v, s in vend_stats.items():
    cart = cartera_cz.get(v,0)
    vend_stats_out[str(v)] = {
        'cart':cart,
        'ccc_m':s['ccc_m'], 'pcc_m':round(s['ccc_m']/cart*100,1) if cart else 0,
        'ccc_a':s['ccc_a'], 'pcc_a':round(s['ccc_a']/cart*100,1) if cart else 0,
        'cob9':s['cob9'],
        'cob_p':round(s['mk_sum']/s['ccc_m'],1) if s['ccc_m'] else 0,
        'imp_m':round(s['imp_m']),
        'cob_abr':cob_abr.get(v,{}),
    }
    # Agregar obj_ccc
    ccc_info = ccc_obj_actual.get(v,{})
    vend_stats_out[str(v)]['obj_ccc'] = ccc_info.get('obj_ccc',0)

print(f"\nGuia: {len(guia)} clientes | Periodos: {len(PERF_DATA)}")

# ─── SERIALIZAR ────────────────────────────────────────────────

print("\nSerializando datos...")
fecha = datetime.now().strftime('%d/%m/%Y %H:%M')

guia_js    = 'const GUIA_DATA='    + json.dumps(guia, ensure_ascii=True, separators=(',',':')) + ';'
abr_js     = 'const ABR_DATA='     + json.dumps(abr_data, ensure_ascii=True, separators=(',',':')) + ';'
otros_js   = 'const OTROS_PROV_DET=' + json.dumps(otros_prov_clean, ensure_ascii=True, separators=(',',':')) + ';\n'
otros_js  += 'const TODOS_PROV_ACTIVOS=' + json.dumps(OTROS_PROV_ACTIVOS, ensure_ascii=True, separators=(',',':')) + ';'
stats_js   = 'const VEND_STATS='   + json.dumps(vend_stats_out, ensure_ascii=True, separators=(',',':')) + ';'
perf_js    = 'const PERF_DATA='    + json.dumps(PERF_DATA, ensure_ascii=True, separators=(',',':')) + ';'

cartera_js = 'const CARTERA_VEND_BASE=' + json.dumps(
    {str(k):v for k,v in cartera_cz.items()}, ensure_ascii=True, separators=(',',':')) + ';'

# ─── GENERAR GUIA DE VISITA ────────────────────────────────────

print("\nGenerando guia_visita_611.html...")
guia_template = os.path.join(BASE_DIR, "guia_template.html")
guia_output   = os.path.join(BASE_DIR, "guia_visita_611.html")

if os.path.exists(guia_template):
    with open(guia_template,'r',encoding='utf-8') as f:
        html_guia = f.read()
    html_guia = html_guia.replace('// __GUIA_DATA__',  guia_js)
    html_guia = html_guia.replace('// __ABR_DATA__',   abr_js)
    html_guia = html_guia.replace('// __OTROS_PROV__', otros_js)
    html_guia = html_guia.replace('// __VEND_STATS__', stats_js)
    html_guia = html_guia.replace('__FECHA_GENERACION__', fecha)
    with open(guia_output,'w',encoding='utf-8') as f:
        f.write(html_guia)
    print(f"  OK: {os.path.getsize(guia_output)/1024:.0f} KB")
else:
    print("  SKIP: guia_template.html no encontrado")

# ─── GENERAR MAS ANALYTICS ────────────────────────────────────

print("\nGenerando mas_analytics_v9.html...")
dash_template = os.path.join(BASE_DIR, "dashboard_template.html")
dash_output   = os.path.join(BASE_DIR, "mas_analytics_v9.html")

if os.path.exists(dash_template):
    with open(dash_template,'r',encoding='utf-8') as f:
        html_dash = f.read()
    html_dash = html_dash.replace('// __PERF_DATA__',    perf_js)
    html_dash = html_dash.replace('// __CARTERA_BASE__', cartera_js)
    html_dash = html_dash.replace('__FECHA_GENERACION__', fecha)
    with open(dash_output,'w',encoding='utf-8') as f:
        f.write(html_dash)
    print(f"  OK: {os.path.getsize(dash_output)/1024:.0f} KB")
else:
    print("  SKIP: mas_template.html no encontrado")

print(f"\n{'='*60}")
print(f"Generacion completada: {fecha}")
print(f"Clientes en guia:  {len(guia)}")
print(f"Periodos en dash:  {list(PERF_DATA.keys())}")
print(f"{'='*60}")
