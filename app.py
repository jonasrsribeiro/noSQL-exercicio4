import json
import uuid
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


cloud_config = {"secure_connect_bundle": "secure-connect-mercadolivre.zip"}

with open("jonasraf97@gmail.com-token.json") as f:
    secrets = json.load(f)

CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["secret"]

auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

session.set_keyspace("mercadolivre")

session.execute(
    """
    CREATE TABLE IF NOT EXISTS usuario (
        id UUID PRIMARY KEY,
        nome TEXT,
        sobrenome TEXT,
        cpf TEXT,
        senha TEXT,
        email TEXT,
        end LIST<FROZEN<MAP<TEXT, TEXT>>>
    )
"""
)

session.execute("""CREATE INDEX IF NOT EXISTS idx_nome ON usuario (nome);""")
session.execute("""CREATE INDEX IF NOT EXISTS idx_nome_vendedor ON vendedor (nome);""")
session.execute("""CREATE INDEX IF NOT EXISTS idx_nome_produto ON produto (nome);""")
session.execute("""CREATE INDEX IF NOT EXISTS idx_nome ON compra (id);""")
session.execute("""CREATE INDEX IF NOT EXISTS idx_nome ON favorito (id);""")


session.execute(
    """
    CREATE TABLE IF NOT EXISTS vendedor (
        id UUID PRIMARY KEY,
        nome TEXT,
        sobrenome TEXT,
        cpf TEXT,
        email TEXT,
        cnpj TEXT,
        end LIST<FROZEN<MAP<TEXT, TEXT>>>
    )
"""
)

session.execute(
    """
    CREATE TABLE IF NOT EXISTS produto (
        id UUID PRIMARY KEY,
        nome TEXT,
        preco DOUBLE,
        descricao TEXT,
        categoria TEXT
    )
"""
)

session.execute(
    """
    CREATE TABLE IF NOT EXISTS compra (
        id UUID PRIMARY KEY,
        data_compra TEXT,
        data_entrega TEXT,
        status_compra TEXT,
        usuario_id UUID,
        produto_id UUID
    )
"""
)

session.execute(
    """
    CREATE TABLE IF NOT EXISTS favorito (
        id UUID PRIMARY KEY,
        usuario_id UUID,
        produto_id UUID
    )
"""
)

""" GET_ALL """


def get_all(table_name):
    query = f"SELECT * FROM {table_name}"
    rows = session.execute(query)
    for row in rows:
        print(f"{table_name.capitalize()}s disponíveis:")
        print("ID:", row.id)
        print("Nome:", row.nome)


""" USUARIO """


def create_usuario():
    nome = input("Nome: ")
    sobrenome = input("Sobrenome: ")
    cpf = input("CPF: ")
    senha = input("Senha: ")
    email = input("Endereço: ")
    endereco = [
        {
            "rua": "Avenida Paulista",
            "num": "123",
            "bairro": "Bela Vista",
            "cidade": "São Paulo",
            "estado": "SP",
            "cep": "01310-000",
        },
        {
            "rua": "Rua Augusta",
            "num": "456",
            "bairro": "Consolação",
            "cidade": "São Paulo",
            "estado": "SP",
            "cep": "01305-000",
        },
    ]
    user_id = uuid.uuid4()

    query = """
        INSERT INTO usuario (id, nome, sobrenome, cpf, senha, email, end)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    session.execute(query, (user_id, nome, sobrenome, cpf, senha, email, endereco))
    print("Usuário inserido com ID", user_id)


def read_usuario(nome):
    query = f"SELECT * FROM usuario WHERE nome = %s"
    rows = session.execute(query, (nome,))
    for row in rows:
        print(row)


def update_usuario(nome):
    query = f"SELECT * FROM usuario WHERE nome = %s"
    rows = list(session.execute(query, (nome,)))

    if not rows:
        print("Usuário não encontrado.")
        return

    user = rows[0]
    novo_nome = input("Mudar Nome:")
    novo_sobrenome = input("Mudar Sobrenome:")
    novo_email = input("Mudar Email:")
    novo_cpf = input("Mudar CPF:")

    query = """
        UPDATE usuario
        SET nome = %s, sobrenome = %s, email = %s, cpf = %s
        WHERE id = %s
    """
    session.execute(query, (novo_nome, novo_sobrenome, novo_email, novo_cpf, user.id))
    print("Usuário atualizado")


def delete_usuario(id):
    query = "DELETE FROM usuario WHERE id = %s"
    session.execute(query, (id,))
    print("Usuário deletado")


""" VENDEDOR """


def create_vendedor():
    nome = input("Nome: ")
    sobrenome = input("Sobrenome: ")
    cpf = input("CPF: ")
    email = input("E-mail: ")
    cnpj = input("CNPJ: ")
    endereco = [
        {
            "rua": "Avenida Paulista",
            "num": "123",
            "bairro": "Bela Vista",
            "cidade": "São Paulo",
            "estado": "SP",
            "cep": "01310-000",
        },
        {
            "rua": "Rua Augusta",
            "num": "456",
            "bairro": "Consolação",
            "cidade": "São Paulo",
            "estado": "SP",
            "cep": "01305-000",
        },
    ]
    vendor_id = uuid.uuid4()

    query = """
        INSERT INTO vendedor (id, nome, sobrenome, cpf, email, cnpj, end)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    session.execute(query, (vendor_id, nome, sobrenome, cpf, email, cnpj, endereco))
    print("Vendedor inserido com ID", vendor_id)


def read_vendedor(nome):
    query = f"SELECT * FROM vendedor WHERE nome = %s ALLOW FILTERING"
    rows = session.execute(query, (nome,))
    for row in rows:
        print(row)


def update_vendedor(nome):
    query = f"SELECT * FROM vendedor WHERE nome = %s ALLOW FILTERING"
    rows = list(session.execute(query, (nome,)))

    if not rows:
        print("Vendedor não encontrado.")
        return

    vendor = rows[0]
    novo_nome = input("Mudar Nome:")
    novo_sobrenome = input("Mudar Sobrenome:")
    novo_email = input("Mudar Email:")
    novo_cpf = input("Mudar CPF:")
    novo_cnpj = input("Mudar CNPJ:")

    query = """
        UPDATE vendedor
        SET nome = %s, sobrenome = %s, email = %s, cpf = %s, cnpj = %s
        WHERE id = %s
    """
    session.execute(
        query, (novo_nome, novo_sobrenome, novo_email, novo_cpf, novo_cnpj, vendor.id)
    )
    print("Vendedor atualizado")


def delete_vendedor(id):
    query = "DELETE FROM vendedor WHERE id = %s"
    session.execute(query, (id,))
    print("Vendedor deletado")

""" PRODUTO """


def create_produto():
    nome = input("Nome do produto: ")
    preco = float(input("Preço do produto: "))
    descricao = input("Descrição do produto: ")
    categoria = input("Categoria do produto: ")
    product_id = uuid.uuid4()

    query = """
        INSERT INTO produto (id, nome, preco, descricao, categoria)
        VALUES (%s, %s, %s, %s, %s)
    """
    session.execute(query, (product_id, nome, preco, descricao, categoria))
    print("Produto inserido com ID", product_id)


def read_produto(nome):
    query = f"SELECT * FROM produto WHERE nome = %s"
    rows = session.execute(query, (nome,))
    for row in rows:
        print(row)


def update_produto(nome):
    query = f"SELECT * FROM produto WHERE nome = %s ALLOW FILTERING"
    rows = list(session.execute(query, (nome,)))

    if not rows:
        print("Produto não encontrado.")
        return

    product = rows[0]
    novo_nome = input("Mudar Nome do produto:")
    novo_preco = float(input("Mudar Preço do produto:"))
    nova_descricao = input("Mudar Descrição do produto:")
    nova_categoria = input("Mudar Categoria do produto:")

    query = """
        UPDATE produto
        SET nome = %s, preco = %s, descricao = %s, categoria = %s
        WHERE id = %s
    """
    session.execute(
        query, (novo_nome, novo_preco, nova_descricao, nova_categoria, product.id)
    )
    print("Produto atualizado")


def delete_produto(id):
    query = "DELETE FROM produto WHERE id = %s"
    session.execute(query, (id,))
    print("Produto deletado")


""" COMPRA """


def create_compra():
    data_compra = input("Data da compra (DD-MM-AAAA): ")
    data_entrega = input("Data de entrega (DD-MM-AAAA): ")
    status_compra = input("Status da compra: ")
    usuario_id = uuid.UUID(input("ID do usuário que realizou a compra: "))
    produto_id = uuid.UUID(input("ID do produto comprado: "))
    purchase_id = uuid.uuid4()

    query = """
        INSERT INTO compra (id, data_compra, data_entrega, status_compra, usuario_id, produto_id)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    session.execute(
        query,
        (purchase_id, data_compra, data_entrega, status_compra, usuario_id, produto_id),
    )
    print("Compra inserida com ID", purchase_id)


def read_compra(compra_id):
    try:
        compra_id = uuid.UUID(compra_id)
    except ValueError:
        print("ID de compra inválido.")
        return

    query = "SELECT * FROM compra WHERE id = %s"
    rows = session.execute(query, (compra_id,))

    for row in rows:
        print(row)


def update_compra(compra_id):
    try:
        compra_id = uuid.UUID(
            compra_id
        )
    except ValueError:
        print("ID de compra inválido.")
        return

    query = f"SELECT * FROM compra WHERE id = %s"
    rows = list(session.execute(query, (compra_id,)))

    if not rows:
        print("Compra não encontrada.")
        return

    compra_atual = rows[0]
    novo_data_compra = input("Mudar Data da compra (DD-MM-AAAA):")
    novo_data_entrega = input("Mudar Data de entrega (DD-MM-AAAA):")
    novo_status_compra = input("Mudar Status da compra:")

    compra_atualizada = compra_atual._replace(
        data_compra=novo_data_compra,
        data_entrega=novo_data_entrega,
        status_compra=novo_status_compra,
    )

    update_query = """
        UPDATE compra
        SET data_compra = %s, data_entrega = %s, status_compra = %s
        WHERE id = %s
    """
    session.execute(
        update_query,
        (
            compra_atualizada.data_compra,
            compra_atualizada.data_entrega,
            compra_atualizada.status_compra,
            compra_id,
        ),
    )
    print("Compra atualizada")


def delete_compra(compra_id):
    query = "DELETE FROM compra WHERE id = %s"
    session.execute(query, (compra_id,))
    print("Compra deletada")


""" FAVORITOS """


def create_favorito():
    get_all("usuario")
    usuario_id = uuid.UUID(input("Digite o ID do usuário: "))
    get_all("produto")
    produto_id = uuid.UUID(input("Digite o ID do produto: "))
    favorite_id = uuid.uuid4()

    query = """
        INSERT INTO favorito (id, usuario_id, produto_id)
        VALUES (%s, %s, %s)
    """
    session.execute(query, (favorite_id, usuario_id, produto_id))
    print("Favorito inserido com ID", favorite_id)


def list_favoritos(usuario_id):
    query = "SELECT * FROM favorito WHERE usuario_id = %s ALLOW FILTERING"
    rows = session.execute(query, (usuario_id,))
    for row in rows:
        print("Produto ID:", row.produto_id)


def delete_favorito(usuario_id, produto_id):
    query = "DELETE FROM favorito WHERE usuario_id = %s AND produto_id = %s"
    session.execute(query, (usuario_id, produto_id))
    print("Favorito deletado")


# CLI
key = 0
sub = 0
while key != "S":
    print("1-CRUD Usuário")
    print("2-CRUD Vendedor")
    print("3-CRUD Produto")
    print("4-CRUD Comprar")
    print("5- CRUD Favoritos")
    key = input("Digite a opção desejada? (S para sair) ")

    if key == "1":
        print("Menu do Usuário")
        print("1-Create Usuário")
        print("2-Read Usuário")
        print("3-Update Usuário")
        print("4-Delete Usuário")
        sub = input("Digite a opção desejada? (V para voltar) ")
        if sub == "1":
            print("Create usuario")
            create_usuario()
        elif sub == "2":
            nome = input("Read usuário, deseja algum nome especifico? ")
            read_usuario(nome)
        elif sub == "3":
            nome = input("Update usuário, deseja algum nome especifico? ")
            update_usuario(nome)
        elif sub == "4":
            print("delete usuario")
            get_all("usuario")
            usuario_id = uuid.UUID(
                input("Digite o ID do usuário para apagá-lo: ")
            )
            delete_usuario(usuario_id)

    elif key == "2":
        print("Menu do Vendedor")
        print("1-Create Vendedor")
        print("2-Read Vendedor")
        print("3-Update Vendedor")
        print("4-Delete Vendedor")
        sub = input("Digite a opção desejada? (V para voltar) ")
        if sub == "1":
            print("Create Vendedor")
            create_vendedor()
        elif sub == "2":
            nome = input("Read usuário, deseja algum nome especifico? ")
            read_vendedor(nome)
        elif sub == "3":
            nome = input("Update usuário, deseja algum nome especifico? ")
            update_vendedor(nome)
        elif sub == "4":
            print("Delete Vendedor")
            get_all("vendedor")
            vendedor_id = uuid.UUID(
                input("Digite o ID do vendedor para apagá-lo: ")
            )
            delete_vendedor(vendedor_id)

    elif key == "3":
        print("Menu do Produto")
        print("1. Create Produto")
        print("2. Read Produto")
        print("3. Update Produto")
        print("4. Delete Produto")
        sub = input("Digite a opção desejada? (V para voltar) ")
        if sub == "1":
            print("Create Produto")
            create_produto()
        elif sub == "2":
            nome = input("Read Produto, deseja algum nome específico? ")
            read_produto(nome)
        elif sub == "3":
            nome = input("Update Produto, deseja algum nome específico? ")
            update_produto(nome)
        elif sub == "4":
            print("Delete Produto")
            get_all("produto")
            produto_id = uuid.UUID(
                input("Digite o ID do produto para apagá-lo: ")
            )
            delete_produto(produto_id)
            

    elif key == "4":
        print("Menu da Compra")
        print("1. Create Compra")
        print("2. Read Compra")
        print("3. Update Compra")
        print("4. Delete Compra")
        sub = input("Digite a opção desejada? (V para voltar) ")
        if sub == "1":
            print("Create Compra")
            create_compra()
        elif sub == "2":
            compra_id = input("Read Compra, deseja algum ID específico? ")
            read_compra(compra_id)
        elif sub == "3":
            compra_id = input("Update Compra, deseja algum ID específico? ")
            update_compra(compra_id)
        elif sub == "4":
            print("Delete Compra")
            compra_id = input("ID a ser deletado: ")
            get_all("compra")
            compra_id = uuid.UUID(
                input("Digite o ID do compra para apagá-lo: ")
            )
            delete_compra(compra_id)

    elif key == "5":
        print("Menu de Favoritos")
        print("1. Adicionar Favorito")
        print("2. Listar Favoritos")
        print("3. Remover Favorito")
        sub = input("Digite a opção desejada? (V para voltar) ")
        if sub == "1":
            print("Adicionar Favorito")
            create_favorito()
        elif sub == "2":
            get_all("usuario")
            usuario_id = uuid.UUID(
                input("Digite o ID do usuário para listar seus favoritos: ")
            )
            list_favoritos(usuario_id)
        elif sub == "3":
            usuario_id = uuid.UUID(input("ID do usuário: "))
            produto_id = uuid.UUID(input("ID do produto: "))
            delete_favorito(usuario_id, produto_id)
    else:
        print("Tchau Prof...")

session.shutdown()
