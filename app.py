import streamlit as st
import pandas as pd
import pymysql
import os
from datetime import datetime
from uuid import uuid4

# ---------------- CONFIG ----------------
REQUIRED_COLS = ["cod_alfa", "price", "quantity"]

st.set_page_config(page_title="Pedidos COMEX", layout="wide")
st.title("Pedidos COMEX")

# ---------------- DB ----------------
def get_conn():
    # Soporta Streamlit Cloud Secrets o variables de entorno
    mysql_cfg = st.secrets.get("mysql", {})
    host = mysql_cfg.get("host") or os.getenv("MYSQL_HOST")
    user = mysql_cfg.get("user") or os.getenv("MYSQL_USER")
    password = mysql_cfg.get("password") or os.getenv("MYSQL_PASSWORD")
    database = mysql_cfg.get("database") or os.getenv("MYSQL_DATABASE")

    if not all([host, user, password, database]):
        raise RuntimeError("Faltan credenciales de MySQL (Secrets o variables de entorno).")

    return pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )

def load_articulos(conn, cods):
    if not cods:
        return pd.DataFrame(columns=["cod_alfa", "proveedor", "nombre"])
    sql = f"""
        SELECT cod_alfa, proveedor, nombre
        FROM articulos_comex
        WHERE cod_alfa IN ({",".join(["%s"] * len(cods))})
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
        rows.append(
            (
                numero,
                str(r["CLIENTE"]),
                r["COD_ALFA"],
                float(r["CANTIDAD"]),
                float(r["PRECIO"]),
                r["rs"],
                item,
                "CX",
                0,
                "N",
                user_email,
                now,
            )
        )
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

def list_pedidos(conn, limit=200):
    # Si querés filtrar solo los de esta app:
    # AND app='CX'
    sql = """
        SELECT NUMERO, MAX(TS) AS last_ts, MAX(rs) AS rs
        FROM sap_comex
        WHERE NUMERO IS NOT NULL AND NUMERO <> ''
        GROUP BY NUMERO
        ORDER BY last_ts DESC
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        return cur.fetchall()

def load_pedido_lines(conn, numero):
    sql = """
        SELECT
            ITEM, COD_ALFA, CANTIDAD, PRECIO, rs, TS, user_email, sap_ready, proc_sap
        FROM sap_comex
        WHERE NUMERO = %s
        ORDER BY ITEM ASC
    """
    with conn.cursor() as cur:
        cur.execute(sql, (numero,))
        return pd.DataFrame(cur.fetchall())

def update_pedido_lines(conn, numero, df_edit):
    """
    df_edit debe tener: ITEM, CANTIDAD, PRECIO
    Actualiza por (NUMERO, ITEM) (tenés uq_doc_item)
    """
    sql = """
        UPDATE sap_comex
        SET CANTIDAD = %s,
            PRECIO   = %s,
            TS       = %s
        WHERE NUMERO = %s AND ITEM = %s
    """
    now = datetime.now()
    payload = []
    for _, r in df_edit.iterrows():
        payload.append(
            (
                float(r["CANTIDAD"]),
                float(r["PRECIO"]),
                now,
                numero,
                int(r["ITEM"]),
            )
        )
    with conn.cursor() as cur:
        cur.executemany(sql, payload)

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

        df = df[REQUIRED_COLS].copy()
        df["cod_alfa"] = df["cod_alfa"].astype(str).str.strip()
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

        bad = df[df["price"].isna() | df["quantity"].isna() | (df["price"] <= 0) | (df["quantity"] <= 0)]
        if len(bad):
            st.error("Hay valores inválidos (price/quantity deben ser numéricos y > 0).")
            st.dataframe(bad, use_container_width=True)
            st.stop()

        conn = get_conn()
        try:
            art = load_articulos(conn, df["cod_alfa"].unique().tolist())
        finally:
            conn.close()

        df2 = df.merge(art, on="cod_alfa", how="left")
        df2["CLIENTE"] = df2["proveedor"]
        df2["rs"] = df2["nombre"]
        df2.rename(columns={"cod_alfa": "COD_ALFA", "price": "PRECIO", "quantity": "CANTIDAD"}, inplace=True)

        st.subheader("Preview")
        st.dataframe(df2, use_container_width=True)

        sin = df2[df2["CLIENTE"].isna()]
        if len(sin):
            st.error("Ítems sin proveedor (falta mapeo en articulos_comex):")
            st.dataframe(sin[["COD_ALFA"]], use_container_width=True)
            st.stop()

        resumen = df2.groupby(["CLIENTE", "rs"], as_index=False).agg(
            items=("COD_ALFA", "count"),
            total_qty=("CANTIDAD", "sum"),
        )
        resumen["total_usd"] = (
            df2.assign(imp=df2["CANTIDAD"] * df2["PRECIO"])
            .groupby(["CLIENTE", "rs"])["imp"]
            .sum()
            .values
        )

        st.subheader("Pedidos a generar (1 por proveedor)")
        st.dataframe(resumen, use_container_width=True)

        if st.button("Confirmar e insertar", type="primary"):
            if not user_email.strip():
                st.error("Ingresá el email del usuario antes de confirmar.")
                st.stop()

            conn = get_conn()
            try:
                for (cli, rs), grp in df2.groupby(["CLIENTE", "rs"]):
                    numero = gen_numero(f"COMEX-P{int(cli)}")
                    insert_lines(conn, grp, numero, user_email.strip())
                conn.commit()
                st.success("Pedidos generados.")
            except Exception as e:
                conn.rollback()
                st.exception(e)
            finally:
                conn.close()

with tab2:
    st.subheader("Editar pedidos (precio y cantidad)")

    conn = get_conn()
    try:
        pedidos = list_pedidos(conn, limit=200)
        if not pedidos:
            st.info("No hay pedidos cargados todavía.")
            st.stop()

        # Dropdown con etiqueta útil
        meta = {}
        options = []
        for p in pedidos:
            numero = p["NUMERO"]
            label = f"{numero}  |  {p.get('rs') or ''}  |  {str(p.get('last_ts') or '')}"
            options.append(label)
            meta[label] = numero

        sel = st.selectbox("Seleccioná un pedido", options, index=0)
        numero = meta[sel]

        df_lines = load_pedido_lines(conn, numero)
        if df_lines.empty:
            st.warning("El pedido no tiene líneas.")
            st.stop()

        st.caption("Editá únicamente CANTIDAD y PRECIO. Guardá para persistir en MySQL.")
        edited = st.data_editor(
            df_lines,
            use_container_width=True,
            hide_index=True,
            disabled=["ITEM", "COD_ALFA", "rs", "TS", "user_email", "sap_ready", "proc_sap"],
            column_config={
                "CANTIDAD": st.column_config.NumberColumn(min_value=0.0, step=1.0, format="%.3f"),
                "PRECIO": st.column_config.NumberColumn(min_value=0.0, step=0.1, format="%.4f"),
            },
            key=f"edit_{numero}",
        )

        # Totales en vivo
        tmp = edited.copy()
        tmp["IMP"] = tmp["CANTIDAD"] * tmp["PRECIO"]
        c1, c2 = st.columns(2)
        c1.metric("Total USD", f"{tmp['IMP'].sum():,.2f}")
        c2.metric("Total Qty", f"{tmp['CANTIDAD'].sum():,.3f}")

        def validate(dfv):
            q = pd.to_numeric(dfv["CANTIDAD"], errors="coerce")
            p = pd.to_numeric(dfv["PRECIO"], errors="coerce")
            if q.isna().any() or p.isna().any():
                return False
            if (q <= 0).any() or (p <= 0).any():
                return False
            return True

        if st.button("💾 Guardar cambios", type="primary"):
            if not validate(edited):
                st.error("Hay valores inválidos (cantidad/precio deben ser numéricos y > 0).")
                st.stop()

            df_to_save = edited[["ITEM", "CANTIDAD", "PRECIO"]].copy()

            try:
                update_pedido_lines(conn, numero, df_to_save)
                conn.commit()
                st.success("Cambios guardados.")
            except Exception as e:
                conn.rollback()
                st.exception(e)

    finally:
        conn.close()
