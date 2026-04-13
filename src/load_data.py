import pandas as pd


def load_pedidos(path):
    return pd.read_excel(path)


def load_catalogo(path):
    return pd.read_excel(path)


def load_matriz(path):
    return pd.read_excel(path, index_col=0)