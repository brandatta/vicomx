import streamlit as st
import pandas as pd
import pymysql
import os
import re
from datetime import datetime
from uuid import uuid4

# ---------------- CONFIG ----------------
REQUIRED_COLS = ["cod_alfa", "price", "quantity"]

st.set_page_config(page_title="Pedidos COMEX", layout="wide")
st.title("Pedidos COMEX")

# ---------------- DB ----------------
def get_conn():
    return pymysql.connect(
        host = st.secrets["mysql"]["host"] if "mysql" in st.secrets else os.getenv("MYSQL_HOST"),
        user = st.secrets["mysql"]["user"] if "mysql" in st.secrets else os.getenv("MYSQL_USER"),
        password = st.secrets["mysql"]["password"] if "mysql" in st.secrets else os.getenv("MYSQL_PASSWORD"),
        database = st.secrets["mysql"]["database"] if "mysql" in st.secrets else os.getenv("MYSQL_DATABASE"),
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor
    )

def load_articulos(conn, cods):
    sql = f"""
        SELECT cod_alfa, proveedor, nombre
        FROM articulos_comex
        WHERE cod_alfa IN ({",".join(["%s"]*len(cods))})
    """
    with conn.cursor() as cur:
        cur.execute(sql, cods)
        return pd.DataFrame(cur.fetchall())

def gen_numero(pref="COMEX"):
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"{pref}-{ts}-{str(uuid4())[:4]}"

def insert_lines(conn, df, numero, user_email):
    now = datetime.now()
    rows = []
    item = 1
    for _, r in df.iterrows():
        rows.append((
            numero,
            str(r["CLIENTE"]),
            r["COD_ALFA"],
            r["CANTIDAD"],
            r["PRECIO"],
            r["rs"],
            item,
            "CX",
            0,
            "N",
            user_email,
            now
        ))
        item += 1

    sql = """
    INSERT INTO sap_comex
      (NUMERO, CLIENTE, COD_ALFA, CANTIDAD, PRECIO, rs,
       ITEM, app, proc_sap, sap_ready, user_email, TS)
    VALUES
      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      CANTIDAD=VALUES(CANTIDAD),
      PRECIO=VALUES(PRECIO),
      rs=VALUES(rs),
      user_email=VALUES(user_email),
      TS=VALUES(TS),
      proc_sap=0,
      sap_ready='N'
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)

# ---------------- UI ----------------
tab1, tab2 = st.tabs(["➕ Nuevo pedido", "📦 Pedidos"])

with tab1:
    user_email = st.text_input("Usuario (email)")
    file = st.file_uploader("Subir Excel", type=["xlsx"])

    if file:
        df = pd.read_excel(file)
        df.columns = [c.strip().lower() for c in df.columns]

        miss = [c for c in REQUIRED_COLS if c not in df.columns]
        if miss:
            st.error(f"Faltan columnas: {miss}")
            st.stop()

        df = df[REQUIRED_COLS]
        df["cod_alfa"] = df["cod_alfa"].astype(str).str.strip()
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

        bad = df[df["price"].isna() | df["quantity"].isna() | (df["price"]<=0) | (df["quantity"]<=0)]
        if len(bad):
            st.error("Hay valores inválidos")
            st.dataframe(bad)
            st.stop()

        conn = get_conn()
        art = load_articulos(conn, df["cod_alfa"].unique().tolist())
        conn.close()

        df2 = df.merge(art, on="cod_alfa", how="left")
        df2["CLIENTE"] = df2["proveedor"]
        df2["rs"] = df2["nombre"]
        df2.rename(columns={"cod_alfa":"COD_ALFA","price":"PRECIO","quantity":"CANTIDAD"}, inplace=True)

        st.subheader("Preview")
        st.dataframe(df2)

        sin = df2[df2["CLIENTE"].isna()]
        if len(sin):
            st.error("Ítems sin proveedor:")
            st.dataframe(sin[["COD_ALFA"]])
            st.stop()

        resumen = df2.groupby(["CLIENTE","rs"],as_index=False).agg(
            items=("COD_ALFA","count"),
            total_qty=("CANTIDAD","sum"),
            total_usd=("PRECIO",lambda s:0)
        )
        resumen["total_usd"] = df2.assign(imp=df2["CANTIDAD"]*df2["PRECIO"]) \
                                  .groupby(["CLIENTE","rs"])["imp"].sum().values

        st.subheader("Pedidos a generar")
        st.dataframe(resumen)

        if st.button("Confirmar e insertar"):
            conn = get_conn()
            try:
                for (cli, rs), grp in df2.groupby(["CLIENTE","rs"]):
                    numero = gen_numero(f"COMEX-P{int(cli)}")
                    insert_lines(conn, grp, numero, user_email)
                conn.commit()
                st.success("Pedidos generados.")
            except Exception as e:
                conn.rollback()
                st.exception(e)
            finally:
                conn.close()

with tab2:
    st.info("Listado de pedidos (pendiente de agregar)")
