import streamlit as st
import pandas as pd
import pymysql
import os
from datetime import datetime
from uuid import uuid4

# ---------------- CONFIG ----------------
REQUIRED_COLS = ["cod_alfa", "price", "quantity"]
DEFAULT_ESTADO_ID = 1  # equivale a "Armado" en comex_estados

st.set_page_config(page_title="Pedidos COMEX", layout="wide")
st.title("Pedidos COMEX")

# ---------------- DB ----------------
def get_conn():
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

# --------- Estados / Meta (según tus DDL) ---------
def get_estados(conn):
    sql = "SELECT id, estado FROM comex_estados ORDER BY id"
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()

def get_estado_texto_por_id(conn, estado_id: int) -> str:
    sql = "SELECT estado FROM comex_estados WHERE id = %s LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(sql, (int(estado_id),))
        row = cur.fetchone()
    if not row:
        raise RuntimeError(f"No existe comex_estados.id={estado_id}.")
    return row["estado"]

def insert_pedido_meta(conn, pedido: str, estado_texto: str, usr: str):
    ts = datetime.now()
    sql = """
        INSERT INTO pedidos_meta_id (pedido, estado, ts, usr)
        VALUES (%s, %s, %s, %s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (pedido, estado_texto, ts, usr))

def get_trazabilidad(conn, pedido: str):
    sql = """
        SELECT id, pedido, estado, ts, usr
        FROM pedidos_meta_id
        WHERE pedido = %s
        ORDER BY ts ASC, id ASC
    """
    with conn.cursor() as cur:
        cur.execute(sql, (pedido,))
        return pd.DataFrame(cur.fetchall())

# --- Pedidos con proveedor + estado actual (para filtros) ---
def get_pedidos_index(conn, limit=1000):
    """
    Index para UI: proveedor (CLIENTE), rs, pedido, ts, estado actual.
    Toma proveedor desde sap_comex (MAX(CLIENTE)) y estado actual desde pedidos_meta_id.
    """
    sql = """
    SELECT
      p.NUMERO AS pedido,
      p.cliente AS proveedor,
      p.rs,
      p.last_ts,
      pm.estado AS estado_texto
    FROM (
      SELECT
        NUMERO,
        MAX(CLIENTE) AS cliente,
        MAX(rs) AS rs,
        MAX(TS) AS last_ts
      FROM sap_comex
      WHERE NUMERO IS NOT NULL AND NUMERO <> ''
      GROUP BY NUMERO
    ) p
    LEFT JOIN (
      SELECT x.pedido, x.estado, x.ts
      FROM pedidos_meta_id x
      INNER JOIN (
        SELECT pedido, MAX(ts) AS max_ts
        FROM pedidos_meta_id
        GROUP BY pedido
      ) y
      ON x.pedido = y.pedido AND x.ts = y.max_ts
    ) pm
      ON pm.pedido = p.NUMERO
    ORDER BY p.last_ts DESC
    LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        return pd.DataFrame(cur.fetchall())

# --------- sap_comex lines ---------
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
tab1, tab2 = st.tabs(["➕ Nuevo Pedido", "📦 Pedidos"])

# =============== TAB 1: NUEVO PEDIDO ===============
with tab1:
    user_email = st.text_input("Usuario", key="email_new")
    file = st.file_uploader("Seleccionar planilla Excel", type=["xlsx"])

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

        st.subheader("Vista Previa")
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

        st.subheader("Pedidos a Generar en vicomx")
        st.dataframe(resumen, use_container_width=True)

        if st.button("Generar en vicomx", type="primary"):
            if not user_email.strip():
                st.error("Ingresá el email del usuario antes de confirmar.")
                st.stop()

            conn = get_conn()
            try:
                estado_inicial = get_estado_texto_por_id(conn, DEFAULT_ESTADO_ID)

                created = []
                for (cli, rs), grp in df2.groupby(["CLIENTE", "rs"]):
                    numero = gen_numero(f"COMEX-P{int(cli)}")
                    insert_lines(conn, grp, numero, user_email.strip())

                    # LOG estado inicial en pedidos_meta_id
                    insert_pedido_meta(conn, pedido=numero, estado_texto=estado_inicial, usr=user_email.strip())

                    created.append({"pedido": numero, "proveedor": int(cli), "rs": rs, "estado": estado_inicial})

                conn.commit()
                st.success("Pedidos generados y registrados en pedidos_meta_id.")
                st.dataframe(pd.DataFrame(created), use_container_width=True)

            except Exception as e:
                conn.rollback()
                st.exception(e)
            finally:
                conn.close()

# =============== TAB 2: PEDIDOS (2 SELECTBOX: PROVEEDOR + PEDIDO) ===============

    user_email_pedidos = st.text_input("Usuario", key="email_pedidos")

    conn = get_conn()
    try:
        estados_rows = get_estados(conn)
        if not estados_rows:
            st.error("La tabla comex_estados no tiene registros.")
            st.stop()
        estados_textos = [r["estado"] for r in estados_rows]

        idx = get_pedidos_index(conn, limit=2000)
        if idx.empty:
            st.info("No hay pedidos cargados todavía.")
            st.stop()

        # -------- Selectores lado a lado: Proveedor (izq) y Pedido (der) --------
        # Proveedor: mostramos "cliente - rs"
        idx["proveedor_label"] = idx["proveedor"].astype(str) + " - " + idx["rs"].fillna("")
        proveedores = idx[["proveedor", "rs", "proveedor_label"]].drop_duplicates().sort_values("proveedor_label")

        cL, cR = st.columns([1, 2])

        with cL:
            prov_label = st.selectbox("Proveedor", proveedores["proveedor_label"].tolist(), index=0)

        prov_row = proveedores[proveedores["proveedor_label"] == prov_label].iloc[0]
        prov_id = prov_row["proveedor"]
        prov_rs = prov_row["rs"]

        pedidos_prov = idx[idx["proveedor"] == prov_id].copy()
        pedidos_prov["estado_texto"] = pedidos_prov["estado_texto"].fillna("(sin estado)")
        pedidos_prov["pedido_label"] = (
            pedidos_prov["pedido"]
            + " | "
            + pedidos_prov["estado_texto"]
            + " | "
            + pedidos_prov["last_ts"].astype(str)
        )

        if pedidos_prov.empty:
            st.warning("Este proveedor no tiene pedidos.")
            st.stop()

        with cR:
            pedido_label = st.selectbox("Pedido", pedidos_prov["pedido_label"].tolist(), index=0)

        pedido_sel = pedidos_prov[pedidos_prov["pedido_label"] == pedido_label].iloc[0]["pedido"]
        estado_actual = pedidos_prov[pedidos_prov["pedido"] == pedido_sel].iloc[0]["estado_texto"]
        if estado_actual == "(sin estado)":
            estado_actual = None

        st.caption(f"Proveedor seleccionado: **{prov_id} - {prov_rs}**")

        st.divider()

        # ------- Trazabilidad -------
        with st.expander("🧾 Detalle / Trazabilidad del pedido", expanded=True):
            traz = get_trazabilidad(conn, pedido_sel)
            if traz.empty:
                st.info("Este pedido no tiene trazabilidad registrada en pedidos_meta_id.")
            else:
                last = traz.iloc[-1]
                st.markdown(
                    f"**Estado Actual:** `{last['estado']}`  \n"
                    f"**Último cambio:** {last['ts']}  \n"
                    f"**Usuario:** {last['usr']}"
                )
                st.dataframe(traz, use_container_width=True, hide_index=True)

        st.divider()

        # ------- Líneas del pedido (editar) -------
        df_lines = load_pedido_lines(conn, pedido_sel)
        if df_lines.empty:
            st.warning("El pedido no tiene líneas.")
            st.stop()

        edited = st.data_editor(
            df_lines,
            use_container_width=True,
            hide_index=True,
            disabled=["ITEM", "COD_ALFA", "rs", "TS", "user_email", "sap_ready", "proc_sap"],
            column_config={
                "CANTIDAD": st.column_config.NumberColumn(min_value=0.0, step=1.0, format="%.3f"),
                "PRECIO": st.column_config.NumberColumn(min_value=0.0, step=0.1, format="%.4f"),
            },
            key=f"edit_{pedido_sel}",
        )

        tmp = edited.copy()
        tmp["IMP"] = tmp["CANTIDAD"] * tmp["PRECIO"]
        c1, c2 = st.columns(2)
        c1.metric("Total USD", f"{tmp['IMP'].sum():,.2f}")
        c2.metric("Total Qty", f"{tmp['CANTIDAD'].sum():,.3f}")

        st.divider()

        # ------- Cambio de estado -------
        if estado_actual in estados_textos:
            default_idx = estados_textos.index(estado_actual)
        else:
            default_idx = 0

        st.subheader("Estado del pedido")
        new_estado = st.selectbox(
            "Seleccioná nuevo estado",
            estados_textos,
            index=default_idx,
            key=f"estado_{pedido_sel}",
        )

        def validate_lines(dfv):
            q = pd.to_numeric(dfv["CANTIDAD"], errors="coerce")
            p = pd.to_numeric(dfv["PRECIO"], errors="coerce")
            if q.isna().any() or p.isna().any():
                return False
            if (q <= 0).any() or (p <= 0).any():
                return False
            return True

        colA, colB = st.columns([1, 1])

        with colA:
            if st.button("💾 Guardar Modificaciones", type="primary"):
                if not validate_lines(edited):
                    st.error("Valores inválidos (cantidad/precio deben ser numéricos y > 0).")
                    st.stop()

                df_to_save = edited[["ITEM", "CANTIDAD", "PRECIO"]].copy()
                try:
                    update_pedido_lines(conn, pedido_sel, df_to_save)
                    conn.commit()
                    st.success("Líneas actualizadas.")
                except Exception as e:
                    conn.rollback()
                    st.exception(e)

        with colB:
            if st.button("🧾 Registrar cambio de estado", type="secondary"):
                if not user_email_pedidos.strip():
                    st.error("Ingresá el email del usuario para registrar el cambio de estado.")
                    st.stop()

                if estado_actual == new_estado:
                    st.info("El pedido ya está en ese estado. No se registró un nuevo movimiento.")
                else:
                    try:
                        insert_pedido_meta(conn, pedido=pedido_sel, estado_texto=new_estado, usr=user_email_pedidos.strip())
                        conn.commit()
                        st.success(f"Estado '{new_estado}' registrado en pedidos_meta_id.")
                    except Exception as e:
                        conn.rollback()
                        st.exception(e)

    finally:
        conn.close()
