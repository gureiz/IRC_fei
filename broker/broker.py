import zmq

# Os bots (REQ) se conectam na porta 5555 e os servidores (REP) na 5556, assim nenhum dos dois precisa saber o endereço do outro, só o do broker.
context = zmq.Context()

# ROUTER: funciona como um REP, mas assíncrono (atende vários clientes ao mesmo tempo)
client_socket = context.socket(zmq.ROUTER)
client_socket.bind("tcp://*:5555")

# DEALER: funciona como um REQ, mas assíncrono. Ele faz o balanceamento de carga, mandando cada requisição para um servidor diferente
server_socket = context.socket(zmq.DEALER)
server_socket.bind("tcp://*:5556")

print("Broker ativo: clientes em 5555, servidores em 5556", flush=True)

# zmq.proxy: repassa as mensagens de um socket para o outro nos dois sentidos
zmq.proxy(client_socket, server_socket)
