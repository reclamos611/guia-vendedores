#!/usr/bin/env python3
"""
generar_todo.py - MAS Analytics 611 Logistica
Lee estructura comercial desde estructura_comercial.xlsx
"""
import pandas as pd
import json, re, os, sys, math
from datetime import datetime

print("=" * 60)
print("MAS Analytics 611 - Generador")
print("=" * 60)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

def si(v, d=0):
    try: return d if (v is None or (isinstance(v, float) and math.isnan(v))) else int(v)
    except: return d
def sf(v, d=0.0):
    try: return d if (v is None or (isinstance(v, float) and math.isnan(v))) else float(v)
    except: return d
def clean(s, n=30):
    s = str(s).strip()
    try: s = s.encode("latin1").decode("utf-8")
    except: pass
    return re.sub(r"[^\x20-\x7E]", "", s)[:n]
ART_PESO = {}  # se llena al leer maestro_articulos
def exkg(art):
    """Extrae peso en kg del artículo usando maestro si está disponible"""
    art = str(art).strip()
    if ART_PESO and art in ART_PESO:
        return ART_PESO[art]
    # Fallback: extraer del nombre
    m = re.search(r"(\d+)\s*Gramos", art)
    if m: return int(m.group(1))/1000
    m = re.search(r"(\d+)\s*g(?:r|rs)?(?:\b|x)", art, re.I)
    if m: return int(m.group(1))/1000
    m = re.search(r"(\d+)g", art.lower())
    if m: return int(m.group(1))/1000
    return 0
def find(name, kw=None):
    name_n = name.lower().replace("_", " ")
    for f in os.listdir(DATA_DIR):
        if not f.endswith(".xlsx"): continue
        fn = f.lower().replace("_", " ")
        if fn == name_n or f.lower() == name.lower():
            return os.path.join(DATA_DIR, f)
    if kw:
        for f in sorted(os.listdir(DATA_DIR)):
            if f.endswith(".xlsx") and kw in f.lower():
                return os.path.join(DATA_DIR, f)
    return None

# ESTRUCTURA COMERCIAL
print("\nLeyendo estructura comercial...")
ec_path = find("estructura_comercial.xlsx", "estructura")
if not ec_path: print("ERROR: estructura_comercial.xlsx no encontrado"); sys.exit(1)
df_ec = pd.read_excel(ec_path)
SUP_MAP = {}; VNOM = {}; SUP_NOM = {}
for _, row in df_ec.iterrows():
    v = si(row["ID Vendedor"]); m = si(row["ID Supervisor"])
    if v <= 0: continue
    SUP_MAP[v] = m; VNOM[v] = clean(row["Vendedor"], 30); SUP_NOM[m] = clean(row["Supervisor"], 30)
print(f"  {len(VNOM)} vendedores | Mesas: {sorted(set(SUP_MAP.values()))}")

MARCAS = ["Lays","Doritos","Cheetos","3D","Pep","Pehuamar","Twistos","Tostitos","Quaker"]
MARCAS_KW = {"Lays":["lays"],"Doritos":["doritos"],"Cheetos":["cheetos"],"3D":["3d"],
    "Pep":["pep comun","pep rueditas","pep "],"Pehuamar":["pehuamar"],
    "Twistos":["twistos"],"Tostitos":["tostitos"],"Quaker":["quaker","avena"]}
TARGETS = {"Lays":94.9,"Doritos":98.0,"Cheetos":83.9,"3D":85.7,
    "Pep":62.3,"Pehuamar":20.7,"Twistos":79.4,"Tostitos":28.4,"Quaker":4.6}
CAT_KEYS = {"lays":"salty_pg","doritos":"salty_pg","cheetos":"salty_pg","3d":"salty_pg",
    "pep ":"salty_pg","pep comun":"salty_pg","pep rueditas":"salty_pg",
    "tostitos":"salty_pg","pehuamar":"salty_pg",
    "twistos":"crackers","quaker":"cereales","avena":"cereales"}
DIA_MAP = {1:"Lunes",2:"Martes",3:"Miercoles",4:"Jueves",5:"Viernes",6:"Sabado"}
MESES_ES = {1:"Ene",2:"Feb",3:"Mar",4:"Abr",5:"May",6:"Jun",7:"Jul",8:"Ago",9:"Sep",10:"Oct",11:"Nov",12:"Dic"}
MESES_FULL = {"enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
    "julio":7,"agosto":8,"septiembre":9,"octubre":10,"noviembre":11,"diciembre":12}
DIAS_HAB_MES = {1:21,2:19,3:21,4:25,5:22,6:20,7:23,8:22,9:22,10:23,11:21,12:20}
# Excluir solo estos de la sección "otros proveedores" (tienen sección propia)
EXCLUIR_PROV_GUIA = ["PepsiCo"]
PROV_MAP = {"Pepsico de Argentina SRL":"PepsiCo","MOLINOS RIO DE LA PLATA SA":"Molinos",
    "SOFTYS ARGENTINA SA":"Softys","GEORGALOS HNOS S A I C A":"Georgalos",
    "DON SATUR SRL":"Don Satur","INDUSTRIAS QUIMICAS Y MINERAS TIMBO SA":"Timbo",
    "BUHL SA":"Buhl","TRES H BEBIDAS S.A.S.":"Tres H","DULCOR SA":"Dulcor",
    "GENOMMA LABORATORIES ARGENTINA SA":"Genomma","JOSE LLENES SACIF":"Llenes","CRONI SA":"Croni"}

def get_cat(art):
    a = str(art).lower()
    for k, v in CAT_KEYS.items():
        if k in a: return v
    return "salty_sb"
def get_prov(p): return PROV_MAP.get(str(p).strip(), str(p).strip()[:20])

# CLIENTE ZONA
print("Leyendo cliente zona...")
zona_path = find("cliente_zona.xlsx", "zona")
if not zona_path: print("ERROR: cliente_zona.xlsx no encontrado"); sys.exit(1)
df_cz = pd.read_excel(zona_path)
if "estado" in df_cz.columns: df_cz = df_cz[df_cz["estado"] == "A"]
dias_map = {}; cartera_cz = {}
for _, row in df_cz.iterrows():
    cid = si(row["cliente_codigo"]); zona = str(si(row.get("zona_codigo", 0)))
    vend = si(row.get("vendedor", 0)) if "vendedor" in df_cz.columns else si(zona[:-1]) if len(zona)>1 else 0
    dia = si(zona[-1]) if len(zona)>0 else 0
    if cid>0 and vend>0:
        if cid not in dias_map: dias_map[cid] = []
        if 1<=dia<=6: dias_map[cid].append([DIA_MAP[dia], vend])
        if vend in SUP_MAP:
            if vend not in cartera_cz: cartera_cz[vend] = set()
            cartera_cz[vend].add(cid)
cartera_cz = {v: len(c) for v, c in cartera_cz.items()}
print(f"  V51={cartera_cz.get(51,0)}, V45={cartera_cz.get(45,0)}")

# MAESTRO
print("Leyendo maestro clientes...")
mc_path = find("maestro_clientes.xlsx", "maestro")
if not mc_path: print("ERROR: maestro_clientes.xlsx no encontrado"); sys.exit(1)
df_mc = pd.read_excel(mc_path)
if "estado" in df_mc.columns: df_mc = df_mc[df_mc["estado"] == "A"]
mc_dict = {}
for _, row in df_mc.iterrows():
    cid = si(row.get("codigo", 0))
    if cid<=0: continue
    vend = si(row.get("vendedor", 0))
    mc_dict[cid] = {"n":clean(row.get("razon_social",""),30),"d":clean(row.get("direccion",""),35),
        "l":clean(row.get("localidad",""),20),"v":vend,"m":SUP_MAP.get(vend,0),"ds":dias_map.get(cid,[])[:3]}
print(f"  {len(mc_dict)} clientes activos")

# MAESTRO DE ARTÍCULOS - peso por artículo
print("Leyendo maestro de artículos...")
art_path = find("maestro_de_articulos.xlsx", "articulos") or find("maestro_articulos.xlsx", "articulos")
if not art_path:
    # buscar por extension .xls también
    for f in os.listdir(DATA_DIR):
        if 'articulo' in f.lower() and (f.endswith('.xlsx') or f.endswith('.xls')):
            art_path = os.path.join(DATA_DIR, f)
            break
ART_PESO = {}  # {descripcion: peso_kg}
if art_path:
    df_art = pd.read_excel(art_path)
    for _, row in df_art.iterrows():
        desc = str(row.get('descripcion','')).strip()
        peso = float(row['peso']) if pd.notna(row.get('peso')) and float(row.get('peso',0) or 0) > 0 else 0
        if desc and peso > 0:
            ART_PESO[desc] = peso
    print(f"  {len(ART_PESO)} artículos con peso")
else:
    print("  AVISO: maestro de artículos no encontrado - usando extracción por nombre")

# OBJETIVOS POR MARCA (cobertura targets)
OBJ_MARCA = {}
obj_marca_path = os.path.join(DATA_DIR, "objetivos_por_marca_pepsico.xlsx")
if os.path.exists(obj_marca_path):
    df_om = pd.read_excel(obj_marca_path)
    for _, row in df_om.iterrows():
        marca = str(row.get("Marca","")).strip()
        if not marca: continue
        OBJ_MARCA[marca] = {}
        for col in df_om.columns:
            if "Obj" in str(col) and pd.notna(row[col]):
                OBJ_MARCA[marca][str(col)] = float(row[col])
    print(f"  Objetivos por marca cargados: {list(OBJ_MARCA.keys())}")
else:
    print("  AVISO: objetivos_por_marca_pepsico.xlsx no encontrado")

# OBJETIVOS
print("Leyendo objetivos...")
OBJ_KG_MESES = {}; OBJ_CCC_MESES = {}
for f in sorted(os.listdir(DATA_DIR)):
    fl = f.lower()
    if not fl.endswith(".xlsx") or "objetivo" not in fl: continue
    for nom, num in MESES_FULL.items():
        if nom not in fl: continue
        path = os.path.join(DATA_DIR, f)
        if "ccc" in fl:
            df_o = pd.read_excel(path); d = {}
            for _, row in df_o.iterrows():
                try:
                    col = "CODIGO" if "CODIGO" in df_o.columns else "CÓDIGO"
                    v = int(row[col])
                    d[v] = {"obj_ccc":round(sf(row["OBJETIVO"])),"universo":si(row["UNIVERSO"])}
                except: pass
            OBJ_CCC_MESES[num] = d; print(f"  CCC {nom}: {len(d)} vend")
        else:
            df_o = pd.read_excel(path); d = {}
            for _, row in df_o.iterrows():
                try:
                    v = int(row["ven_id"]); cat = str(row["SubGrupoCupo_Descripcion"]); kg = int(row["objetivo"])
                    if v not in d: d[v] = {"pg":0,"sb":0}
                    if "Platino" in cat or "Gold" in cat: d[v]["pg"] = kg
                    elif "Silver" in cat or "Bronze" in cat: d[v]["sb"] = kg
                except: pass
            OBJ_KG_MESES[num] = d; print(f"  KG {nom}: {len(d)} vend")
        break

# DETECTAR VENTAS
print("\nDetectando archivos de venta...")
def detect_mes(path):
    try:
        df_s = pd.read_excel(path, usecols=["Fecha"], nrows=200)
        fechas = pd.to_datetime(df_s["Fecha"], errors="coerce").dropna()
        if len(fechas): p = fechas.mode()[0]; return p.month, p.year
    except: pass
    return None, None

ventas = {}
for f in os.listdir(DATA_DIR):
    if not f.endswith(".xlsx") or f.startswith("~"): continue
    fl = f.lower()
    if any(k in fl for k in ["maestro","zona","objetivo","estructura"]): continue
    path = os.path.join(DATA_DIR, f)
    mes, anio = detect_mes(path)
    if mes and anio:
        key = f"{anio}-{mes:02d}"
        if key not in ventas or os.path.getmtime(path) > os.path.getmtime(ventas[key]):
            ventas[key] = path; print(f"  {f} -> {MESES_ES.get(mes,str(mes))} {anio}")

if not ventas: print("ERROR: No hay archivos de venta"); sys.exit(1)
mes_act_key = sorted(ventas.keys())[-1]

# PROCESAR VENTAS
def procesar(path, excluir_creativa=True):
    print(f"  {os.path.basename(path)} (creativa={'NO' if excluir_creativa else 'SI'})...")
    df = pd.read_excel(path, usecols=["Cliente","Fecha","Cantidad","Importe","camion",
        "proveedor","articulo","cod_ven","tipo_venta","Razon_Social"])
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    # Filtrar solo hasta hoy - excluir fechas futuras (pedidos planificados)
    hoy = pd.Timestamp(datetime.now().date())
    df = df[df["Fecha"] <= hoy].copy()
    df["_v"] = df["cod_ven"].apply(si); df["_cid"] = df["Cliente"].apply(si)
    df["_art"] = df["articulo"].str.lower().fillna("")
    df["_pep"] = df["proveedor"].str.contains("Pepsico", case=False, na=False)
    df["_prov"]= df["proveedor"].apply(get_prov)
    df_pep = df[df["_pep"] & (df["camion"]<700 if excluir_creativa else True)].copy()
    df_real= df[df["camion"]<700 if excluir_creativa else pd.Series([True]*len(df))].copy()
    dias_trab = df_pep["Fecha"].dt.date.nunique() if len(df_pep) else 1
    # CCC = cliente con neto de unidades >= 3 (venta - devolucion - cambio)
    df_vta = df_pep[df_pep["tipo_venta"]=="Venta"]
    df_dev = df_pep[df_pep["tipo_venta"].isin(["Devolucion","Cambio"])]
    uds_vta = df_vta.groupby("_cid")["Cantidad"].sum()
    uds_dev = df_dev.groupby("_cid")["Cantidad"].sum().abs()
    neto_u = uds_vta.subtract(uds_dev, fill_value=0)
    neto_i = df_pep.groupby("_cid")["Importe"].sum()
    ccc_set = set(neto_u[neto_u >= 3].index)
    vend_acc = {}
    for _, row in df_pep.iterrows():
        v = si(row["cod_ven"])
        if v not in SUP_MAP: continue
        imp=sf(row["Importe"]); qty=sf(row["Cantidad"]); art=row["_art"]
        kg=qty*exkg(row["articulo"]); cat=get_cat(art); tipo=str(row.get("tipo_venta",""))
        cid=si(row["Cliente"]); sign=1 if tipo=="Venta" else -1 if tipo=="Devolucion" else 0
        if v not in vend_acc:
            vend_acc[v]={"pv":0,"pd":0,"pc":0,"kg":0,
                "cat":{"salty_pg":0,"salty_sb":0,"crackers":0,"cereales":0},"ccc":set()}
        d=vend_acc[v]
        if tipo=="Venta": d["pv"]+=imp
        elif tipo=="Devolucion": d["pd"]+=abs(imp)
        elif tipo=="Cambio": d["pc"]+=abs(imp)
        d["kg"]+=kg*sign; d["cat"][cat]+=kg*sign
        if cid in ccc_set: d["ccc"].add(cid)
    cli_mk = {}
    for _, row in df_pep.iterrows():
        if str(row.get("tipo_venta",""))=="Devolucion": continue
        cid=si(row["Cliente"]); v=si(row["cod_ven"])
        if v not in SUP_MAP: continue
        if cid not in cli_mk: cli_mk[cid]=[0]*9
        art=row["_art"]
        for idx,mk in enumerate(MARCAS):
            if any(k in art for k in MARCAS_KW[mk]): cli_mk[cid][idx]+=sf(row["Cantidad"]); break
    mk_cob_vend = {}
    for cid,uds in cli_mk.items():
        mc=mc_dict.get(cid,{}); v=mc.get("v",0)
        if not v: continue
        if v not in mk_cob_vend: mk_cob_vend[v]={mk:0 for mk in MARCAS}
        for idx,mk in enumerate(MARCAS):
            if uds[idx]>=3: mk_cob_vend[v][mk]+=1
    otros = {}
    for _, row in df_real.iterrows():
        v=si(row["_v"])
        if v not in SUP_MAP: continue
        prov=row["_prov"]
        if prov in EXCLUIR_PROV_GUIA: continue
        cid=str(si(row["_cid"])); qty=sf(row["Cantidad"]); imp=sf(row["Importe"])
        tipo=str(row.get("tipo_venta",""))
        if tipo=="Devolucion": qty=-abs(qty); imp=-abs(imp)
        if cid not in otros: otros[cid]={}
        if prov not in otros[cid]: otros[cid][prov]=[0,0]
        otros[cid][prov][0]+=qty; otros[cid][prov][1]+=imp
    return {"vend_acc":vend_acc,"cli_mk":cli_mk,"mk_cob_vend":mk_cob_vend,
            "ccc_set":ccc_set,"otros":otros,"dias_trab":dias_trab}

def construir(datos, mes_num, anio, etiq):
    vend_acc=datos["vend_acc"]; mk_cob_vend=datos["mk_cob_vend"]
    dias_trab=datos["dias_trab"]; dias_hab=DIAS_HAB_MES.get(mes_num,22)
    obj_kg=OBJ_KG_MESES.get(mes_num,{}); obj_ccc=OBJ_CCC_MESES.get(mes_num,{})
    perf=[]
    for v in sorted(SUP_MAP.keys()):
        mesa=SUP_MAP[v]; cart=cartera_cz.get(v,0); d=vend_acc.get(v,{})
        okg=obj_kg.get(v,{"pg":0,"sb":0}); obj_tot=okg["pg"]+okg["sb"]
        occc=obj_ccc.get(v,{}); obj_ccc_n=occc.get("obj_ccc",round(cart*0.7))
        kr=round(d.get("kg",0),2); pv=round(d.get("pv",0)); n_ccc=len(d.get("ccc",set()))
        tend=round(kr/dias_trab*dias_hab,2) if dias_trab else 0
        cat=d.get("cat",{}); mk_c=mk_cob_vend.get(v,{mk:0 for mk in MARCAS})
        perf.append({"cod":v,"nom":VNOM.get(v,f"V{v}"),"mesa":mesa,"sup":SUP_NOM.get(mesa,""),"ton":round(kr/1000,3),"imp":pv,"cli":n_ccc,"ticket":round(pv/n_ccc) if n_ccc else 0,
            "cart":cart,"ccc":n_ccc,"pcc":round(n_ccc/cart*100,1) if cart else 0,
            "obj_ccc":obj_ccc_n,"apc":round(n_ccc/obj_ccc_n*100,1) if obj_ccc_n else 0,
            "kr":kr,"ot":obj_tot,"apr":round(kr/obj_tot*100,1) if obj_tot else 0,
            "tend":tend,"apt":round(tend/obj_tot*100,1) if obj_tot else 0,
            "pv":pv,"pd":round(d.get("pd",0)),"pc":round(d.get("pc",0)),
            "pdp":round(d.get("pd",0)/pv*100,2) if pv else 0,
            "cat":{"salty_pg":{"r":round(cat.get("salty_pg",0),1),"o":okg["pg"]},
                   "salty_sb":{"r":round(cat.get("salty_sb",0),1),"o":okg["sb"]},
                   "crackers":{"r":round(cat.get("crackers",0),1),"o":0},
                   "cereales":{"r":round(cat.get("cereales",0),1),"o":0}},
            "mk_cob":mk_c})
    por_mesa={}; mesas_resumen={}
    for mesa in sorted(set(SUP_MAP.values())):
        pvs=[p for p in perf if p["mesa"]==mesa]
        if not pvs: continue
        cart_m=sum(p["cart"] for p in pvs); ccc_m=sum(p["ccc"] for p in pvs)
        kr_m=sum(p["kr"] for p in pvs); ot_m=sum(p["ot"] for p in pvs)
        pv_m=sum(p["pv"] for p in pvs); tend_m=round(kr_m/dias_trab*dias_hab,1) if dias_trab else 0
        cob_m={mk:0 for mk in MARCAS}
        for cid,uds in datos["cli_mk"].items():
            if mc_dict.get(cid,{}).get("m",0)==mesa:
                for idx,mk in enumerate(MARCAS):
                    if uds[idx]>=3: cob_m[mk]+=1
        por_mesa[str(mesa)]={"kpis":{"cartera":cart_m,"ccc":ccc_m,"ccc_pep":ccc_m,
            "cobertura_cartera":round(ccc_m/cart_m*100,1) if cart_m else 0,
            "toneladas":round(kr_m/1000,2),"ton":round(kr_m/1000,3),"importe":pv_m,"imp":pv_m,"unidades":0,"uds":0,
            "ticket":round(pv_m/ccc_m) if ccc_m else 0,"mix_imp":0,"mix":0},
            "cobertura_marcas":{mk:{"clientes":cob_m[mk],"pct":round(cob_m[mk]/cart_m*100,1) if cart_m else 0,"objetivo":TARGETS.get(mk,0)} for mk in MARCAS},"cob":{mk:{"clientes":cob_m[mk],"pct":round(cob_m[mk]/cart_m*100,1) if cart_m else 0,"objetivo":TARGETS.get(mk,0)} for mk in MARCAS},
            "vendedores":pvs}
        mesas_resumen[str(mesa)]={"kg_real":kr_m,"obj_total":ot_m,
            "avance_pct":round(kr_m/ot_m*100,1) if ot_m else 0,
            "imp_real":pv_m,"ccc":ccc_m,"cartera":cart_m,
            "pct_ccc":round(ccc_m/cart_m*100,1) if cart_m else 0,
            "tend":tend_m,"apt":round(tend_m/ot_m*100,1) if ot_m else 0}
    pvs_g=[p for p in perf if p["mesa"] in [300,400,500]]
    tot_cart=sum(p["cart"] for p in pvs_g); tot_ccc=sum(p["ccc"] for p in pvs_g)
    tot_kr=sum(p["kr"] for p in pvs_g); tot_pv=sum(p["pv"] for p in pvs_g)
    cob_g={mk:0 for mk in MARCAS}
    for cid,uds in datos["cli_mk"].items():
        if mc_dict.get(cid,{}).get("m",0) in [300,400,500]:
            for idx,mk in enumerate(MARCAS):
                if uds[idx]>=3: cob_g[mk]+=1
    vendedores_flat=[p for p in perf]
    return {"periodo":etiq,"vendedores":vendedores_flat,
        "cobertura":{mk:{"clientes":cob_g[mk],"pct":round(cob_g[mk]/tot_cart*100,1) if tot_cart else 0,
            "objetivo":TARGETS.get(mk,0)} for mk in MARCAS},
        "kpis_global":{"cartera":tot_cart,"ccc":tot_ccc,"ccc_pep":tot_ccc,
        "cobertura_cartera":round(tot_ccc/tot_cart*100,1) if tot_cart else 0,
        "toneladas":round(tot_kr/1000,2),"importe":tot_pv,"unidades":0,
        "ticket":round(tot_pv/tot_ccc) if tot_ccc else 0,"mix_imp":0},
        "cobertura_marcas":{mk:{"clientes":cob_g[mk],"pct":round(cob_g[mk]/tot_cart*100,1) if tot_cart else 0,
            "objetivo":TARGETS.get(mk,0)} for mk in MARCAS},
        "por_mesa":por_mesa,"mesas_resumen":mesas_resumen,"dias_hab":dias_hab,"dias_trab":dias_trab}

print("\nProcesando ventas...")
datos_meses={}
for key in sorted(ventas.keys()):
    anio,mes=int(key.split("-")[0]),int(key.split("-")[1])
    d=procesar(ventas[key], excluir_creativa=True)
    d["anio"]=anio; d["mes"]=mes; datos_meses[key]=d
    print(f"  {MESES_ES[mes]} {anio}: {d['dias_trab']}d, CCC={len(d['ccc_set'])}")

datos_act=datos_meses[mes_act_key]
anio_act,mes_act=datos_act["anio"],datos_act["mes"]
etiq_act=f"{MESES_ES[mes_act]} {anio_act}"

print("Procesando con venta creativa (toggle)...")
datos_crea=procesar(ventas[mes_act_key], excluir_creativa=False)

DATA_MARZO=construir(datos_act,mes_act,anio_act,etiq_act)
DATA_MARZO_CREA=construir(datos_crea,mes_act,anio_act,etiq_act)
DATA_PERIODOS={etiq_act:DATA_MARZO}
for key in sorted(ventas.keys())[:-1]:
    d=datos_meses[key]; etiq_h=f"{MESES_ES[d['mes']]} {d['anio']}"
    DATA_PERIODOS[etiq_h]=construir(d,d["mes"],d["anio"],etiq_h)
CARTERA_VEND_BASE={str(k):v for k,v in cartera_cz.items()}
print(f"  Periodos: {list(DATA_PERIODOS.keys())}")

# GUIA
print("\nConstruyendo guia...")
datos_ant_d=datos_meses[sorted(ventas.keys())[-2]] if len(ventas)>1 else datos_act
GUIA_DATA={}
for cid,mc in mc_dict.items():
    v=mc["v"]; mesa=mc["m"]
    if mesa==0 or mesa==600: continue
    uds_ant=[max(0,round(x)) for x in datos_ant_d["cli_mk"].get(cid,[0]*9)]
    GUIA_DATA[str(cid)]={"n":mc["n"],"l":mc["l"],"d":mc["d"],"v":v,"m":mesa,"ds":mc["ds"],"u":uds_ant,"i":0}
ABR_DATA={str(cid):[max(0,round(x)) for x in uds] for cid,uds in datos_act["cli_mk"].items()}
VEND_STATS={}
for v in SUP_MAP:
    if SUP_MAP[v]==600: continue
    cart=cartera_cz.get(v,0)
    if not cart: continue
    d_ant=datos_ant_d["vend_acc"].get(v,{}); d_act=datos_act["vend_acc"].get(v,{})
    ccc_m=len(d_ant.get("ccc",set())); ccc_a=len(d_act.get("ccc",set()))
    cob9=sum(1 for cid,uds in datos_ant_d["cli_mk"].items()
             if mc_dict.get(cid,{}).get("v")==v and sum(1 for u in uds if u>=3)==9)
    cob_abr=datos_act["mk_cob_vend"].get(v,{mk:0 for mk in MARCAS})
    VEND_STATS[str(v)]={"cart":cart,"ccc_m":ccc_m,"pcc_m":round(ccc_m/cart*100,1) if cart else 0,
        "ccc_a":ccc_a,"pcc_a":round(ccc_a/cart*100,1) if cart else 0,"cob9":cob9,
        "cob_p":0,"cob_abr":cob_abr,"imp_m":round(d_ant.get("pv",0)),
        "obj_ccc":OBJ_CCC_MESES.get(mes_act,{}).get(v,{}).get("obj_ccc",0)}
# Calcular lista global de proveedores activos (todos los que vendieron algo)
todos_prov_set = set()
for cid_provs in datos_act["otros"].values():
    todos_prov_set.update(cid_provs.keys())
if datos_ant_d != datos_act:
    for cid_provs in datos_ant_d["otros"].values():
        todos_prov_set.update(cid_provs.keys())
todos_prov_activos = sorted(todos_prov_set)
print(f"  Proveedores activos: {todos_prov_activos}")

# Construir otros_clean - para TODOS los clientes de la cartera, mostrar TODOS los proveedores
otros_clean={}
for cid in mc_dict:
    mc = mc_dict[cid]
    if mc.get("m",0) == 0 or mc.get("m",0) == 600: continue
    cid_s = str(cid)
    provs_act = datos_act["otros"].get(cid_s, {})
    provs_ant = datos_ant_d["otros"].get(cid_s, {}) if datos_ant_d!=datos_act else {}
    entry={}
    for p in todos_prov_activos:
        a=provs_act.get(p,[0,0]); b=provs_ant.get(p,[0,0])
        entry[p]=[round(b[0]),round(b[1]),round(a[0]),round(a[1])]
    otros_clean[cid_s]=entry
print(f"  {len(GUIA_DATA)} clientes, {len(VEND_STATS)} vendedores")

# RECHAZO_DATA - Rechazo (Devolucion) e Invendible (Cambio negativo) por vendedor/proveedor
print("\nCalculando rechazo/invendibles por proveedor...")
RECHAZO_DATA = {}
for key in sorted(ventas.keys()):
    d = datos_meses[key]; anio_r,mes_r = d["anio"],d["mes"]
    etiq_r = f"{MESES_ES[mes_r]} {anio_r}"
    df_r = pd.read_excel(ventas[key], usecols=["Cantidad","Importe","camion",
        "proveedor","cod_ven","tipo_venta"])
    df_r["Fecha"] = pd.to_datetime(df_r.get("Fecha",""), errors="coerce") if "Fecha" in df_r.columns else pd.NaT
    df_r["_prov"] = df_r["proveedor"].apply(get_prov)
    # Acumular por vendedor y proveedor
    # pv = venta $, pd = rechazo $ (Devolucion), pc = invendible $ (Cambio con Importe<0)
    vend_prov = {}
    for _, row in df_r.iterrows():
        v = si(row["cod_ven"])
        if v not in SUP_MAP: continue
        prov = row["_prov"]
        imp = sf(row["Importe"])
        tipo = str(row.get("tipo_venta",""))
        if v not in vend_prov: vend_prov[v] = {"pv":{},"pd":{},"pc":{}}
        vp = vend_prov[v]
        if tipo == "Venta" and imp > 0:
            vp["pv"][prov] = vp["pv"].get(prov,0) + imp
        elif tipo == "Devolucion" and imp < 0:
            # Rechazo: sumar el valor absoluto
            vp["pd"][prov] = vp["pd"].get(prov,0) + abs(imp)
        elif tipo == "Cambio" and imp < 0:
            # Invendible: sumar el valor absoluto
            vp["pc"][prov] = vp["pc"].get(prov,0) + abs(imp)
    perf_rec = []
    for v in sorted(SUP_MAP.keys()):
        mesa = SUP_MAP[v]; vp = vend_prov.get(v,{})
        pv     = round(sum(vp.get("pv",{}).values()))
        pd_tot = round(sum(vp.get("pd",{}).values()))
        pc_tot = round(sum(vp.get("pc",{}).values()))
        if pv == 0 and pd_tot == 0 and pc_tot == 0: continue
        perf_rec.append({
            "cod":v, "nom":VNOM.get(v,f"V{v}"), "mesa":mesa, "sup":SUP_NOM.get(mesa,""),
            "pv":pv, "pd":pd_tot, "pc":pc_tot,
            "pdp":round(pd_tot/pv*100,2) if pv else 0,
            "pcp":round(pc_tot/pv*100,2) if pv else 0,
            "prov_venta": {p:round(v2) for p,v2 in vp.get("pv",{}).items()},
            "prov_devol": {p:round(v2) for p,v2 in vp.get("pd",{}).items()},
            "prov_cambio":{p:round(v2) for p,v2 in vp.get("pc",{}).items()},
        })
    RECHAZO_DATA[etiq_r] = {"perf":perf_rec,"stats":{}}
    print(f"  {etiq_r}: {len(perf_rec)} vendedores")

# CREA_DATA - clientes que necesitan venta creativa en Lays/Doritos/Cheetos/3D
print("\nCalculando venta creativa necesaria...")
MARCAS_CREA_KW = {"Lays":"lays","Doritos":"doritos","Cheetos":"cheetos","3D":"3d"}
ART_SUGERIDO_CREA = {
    "Lays":    {"art":"Lays Clasicas 40gx68x1",      "codigo":"300059432","precio":929.75},
    "Doritos": {"art":"Doritos Queso 40gx70x1",       "codigo":"300059545","precio":929.75},
    "Cheetos": {"art":"Cheetos Queso 43gx70x1",       "codigo":"300059433","precio":929.75},
    "3D":      {"art":"3d Queso 43gx75x1","codigo":"300058395","precio":929.75},
}
# Usar datos del mes activo (sin creativa)
key_act = sorted(ventas.keys())[-1]
df_crea_src = pd.read_excel(ventas[key_act], usecols=["Cliente","Fecha","Cantidad","camion",
    "proveedor","articulo","cod_ven","tipo_venta"])
df_crea_src["Fecha"] = pd.to_datetime(df_crea_src["Fecha"], errors="coerce")
hoy_crea = pd.Timestamp(datetime.now().date())
# Para CREA_DATA: incluir TODA la venta (real + creativa) para no repetir
# Si ya se hizo creativa de una marca, el neto ya >= 3 y no aparece en la lista
df_crea_src = df_crea_src[(df_crea_src["Fecha"]<=hoy_crea) &
    df_crea_src["proveedor"].str.contains("Pepsico",case=False,na=False)].copy()

def get_marca_crea(art):
    a = str(art).lower()
    for mk,kw in MARCAS_CREA_KW.items():
        if kw in a: return mk
    return None

df_crea_src["_marca"] = df_crea_src["articulo"].apply(get_marca_crea)
df_crea_src["_sign"] = df_crea_src["tipo_venta"].apply(
    lambda t: 1 if t=="Venta" else -1 if t in ["Devolucion","Cambio"] else 0)
df_crea_src["_neto"] = df_crea_src["Cantidad"]*df_crea_src["_sign"]

# Vendedor principal por cliente
vend_cli_crea = df_crea_src[df_crea_src["cod_ven"].apply(si).isin(SUP_MAP)].groupby(
    "Cliente")["cod_ven"].agg(lambda x: si(x.mode()[0]))

CREA_DATA = []
for marca in MARCAS_CREA_KW:
    dm = df_crea_src[df_crea_src["_marca"]==marca]
    neto_cli = dm.groupby("Cliente")["_neto"].sum()
    for cid, neto in neto_cli[neto_cli<3].items():
        neto = max(0.0, float(neto))
        qty = 3 - int(neto)
        vend = int(vend_cli_crea.get(cid, 0))
        if vend not in SUP_MAP or SUP_MAP[vend]==600: continue
        art = ART_SUGERIDO_CREA[marca]
        CREA_DATA.append({"cliente":int(si(cid)),"vendedor":vend,"mesa":SUP_MAP[vend],
            "marca":marca,"articulo":art["art"],"codigo_art":art["codigo"],
            "precio":art["precio"],"neto_actual":round(neto,0),"cantidad_crea":qty})

print(f"  {len(CREA_DATA)} registros de venta creativa ({len(set(r['cliente'] for r in CREA_DATA))} clientes)")

# SERIALIZAR
print("\nSerializando...")
fecha=datetime.now().strftime("%d/%m/%Y %H:%M")
dm_js="const DATA_MARZO=\n"+json.dumps(DATA_MARZO,ensure_ascii=True,separators=(",",":"))+  ";"
dmc_js="const DATA_MARZO_CREA=\n"+json.dumps(DATA_MARZO_CREA,ensure_ascii=True,separators=(",",":"))+  ";"
dp_js="const DATA_PERIODOS="+json.dumps(DATA_PERIODOS,ensure_ascii=True,separators=(",",":"))+  ";"
cv_js="var CARTERA_VEND_BASE="+json.dumps(CARTERA_VEND_BASE,ensure_ascii=True,separators=(",",":"))+  ";"
rec_js="var RECHAZO_DATA="+json.dumps(RECHAZO_DATA,ensure_ascii=True,separators=(",",":"))+";"
crea_js="const CREA_DATA="+json.dumps(CREA_DATA,ensure_ascii=True,separators=(",",":"))+";"
obj_marca_js="const OBJ_MARCA_DATA="+json.dumps(OBJ_MARCA,ensure_ascii=True,separators=(",",":"))+";"
datos_js="\n".join([dm_js,dmc_js,dp_js,cv_js,rec_js,crea_js,obj_marca_js])
guia_js="const GUIA_DATA="+json.dumps(GUIA_DATA,ensure_ascii=True,separators=(",",":"))+  ";"
abr_js="const ABR_DATA="+json.dumps(ABR_DATA,ensure_ascii=True,separators=(",",":"))+  ";"
stats_js="const VEND_STATS="+json.dumps(VEND_STATS,ensure_ascii=True,separators=(",",":"))+  ";"
otros_js=("const OTROS_PROV_DET="+json.dumps(otros_clean,ensure_ascii=True,separators=(",",":"))+";\n"
    +"const TODOS_PROV_ACTIVOS="+json.dumps(todos_prov_activos,ensure_ascii=True,separators=(",",":"))+  ";")

# GUIA HTML
guia_tmpl=os.path.join(BASE_DIR,"guia_template.html")
if os.path.exists(guia_tmpl):
    with open(guia_tmpl,"r",encoding="utf-8") as f: html=f.read()
    html=html.replace("// __GUIA_DATA__",guia_js).replace("// __ABR_DATA__",abr_js)
    html=html.replace("// __OTROS_PROV__",otros_js).replace("// __VEND_STATS__",stats_js)
    # Actualizar SUP_MAP_BASE y MESA_SUPS desde estructura_comercial
    sup_map_js = "const SUP_MAP_BASE = {\n"
    for v, mesa in sorted(SUP_MAP.items()):
        sup = SUP_NOM.get(mesa,"")
        sup_map_js += f"  {v}:{{sup:'{sup}',mesa:{mesa}}},"
        if v % 10 == 9 or v == sorted(SUP_MAP.keys())[-1]:
            sup_map_js += "\n"
    sup_map_js += "};"
    mesa_sups_js = "const MESA_SUPS = {" + ",".join(
        f"{m}:'{SUP_NOM.get(m,'')}'" for m in sorted(set(SUP_MAP.values()))) + "};"
    tab_300 = next((f"Mesa 300 — {n.title()}" for m,n in SUP_NOM.items() if m==300), "Mesa 300")
    tab_400 = next((f"Mesa 400 — {n.title()}" for m,n in SUP_NOM.items() if m==400), "Mesa 400")
    tab_500 = next((f"Mesa 500 — {n.title()}" for m,n in SUP_NOM.items() if m==500), "Mesa 500")
    import re as _re2
    html = _re2.sub(r"const SUP_MAP_BASE = \{[^}]+(?:\{[^}]*\}[^}]*)*\};", sup_map_js, html)
    html = _re2.sub(r"const MESA_SUPS = \{[^}]+\};", mesa_sups_js, html)
    html = html.replace("Mesa 300 — Natalia Perez", tab_300)
    html = html.replace("Mesa 400 — Claudio Alvarado", tab_400)
    html = html.replace("Mesa 500 — Sebastian Sanchez", tab_500)
    html=html.replace("__FECHA_GENERACION__",fecha)
    # Timestamp único para invalidar cache del browser
    import hashlib
    ts = str(int(__import__("time").time()))
    html = html.replace("</title>", f"</title><!-- v{ts} -->")
    with open(os.path.join(BASE_DIR,"guia_visita_611.html"),"w",encoding="utf-8") as f: f.write(html)
    print(f"Guia: {os.path.getsize(os.path.join(BASE_DIR,'guia_visita_611.html'))//1024}KB")

# DASHBOARD HTML
dash_tmpl=os.path.join(BASE_DIR,"dashboard_template.html")
dash_out=os.path.join(BASE_DIR,"mas_analytics_v9.html")
if os.path.exists(dash_tmpl):
    with open(dash_tmpl,"r",encoding="utf-8") as f: html=f.read()
    import re as _re
    scripts=list(_re.finditer(r"<script>",html))
    ends=[html.find("</script>",s.end()) for s in scripts]
    first_marker=-1; last_marker_end=-1
    for i,s in enumerate(scripts):
        sc=html[s.end():ends[i]].strip()
        if any(x in sc for x in ["__PERF_DATA__","__DATA_PERIODOS__","__CARTERA_DATA__"]):
            if first_marker==-1: first_marker=s.start()
            last_marker_end=ends[i]+len("</script>")
    if first_marker>=0:
        html=html[:first_marker]+"<script>\n"+datos_js+"\n</script>\n"+html[last_marker_end:]
    html=html.replace("__FECHA_GENERACION__",fecha)
    # Agregar meta no-cache para evitar que el browser use versión vieja
    no_cache_meta = (
        '<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">\n'
        '<meta http-equiv="Pragma" content="no-cache">\n'
        '<meta http-equiv="Expires" content="0">\n'
    )
    html = html.replace('<meta charset="UTF-8">', '<meta charset="UTF-8">\n' + no_cache_meta, 1)
    with open(dash_out,"w",encoding="utf-8") as f: f.write(html)
    print(f"Dashboard: {os.path.getsize(dash_out)//1024}KB")

print(f"\n{'='*60}\nCompletado: {fecha}\nPeriodos: {list(DATA_PERIODOS.keys())}\n{'='*60}")
