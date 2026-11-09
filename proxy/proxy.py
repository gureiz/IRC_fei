import zmq

# Os servidores (PUB) se conectam na porta 5557 e os bots (SUB) na 5558.
context = zmq.Context()

# XPUB: lado onde os subscribers (bots) se conectam
pub = context.socket(zmq.XPUB)
pub.bind("tcp://*:5558")

# XSUB: lado onde os publishers (servidores) se conectam
sub = context.socket(zmq.XSUB)
sub.bind("tcp://*:5557")

print("Proxy ativo: publishers em 5557 (XSUB), subscribers em 5558 (XPUB)", flush=True)

# repassa as publicações para os subscribers e as inscrições para os publishers
zmq.proxy(pub, sub)
