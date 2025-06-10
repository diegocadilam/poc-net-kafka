# Objetivo
Um console application apenas para exemplificar o envio de um produto, serializado, para fila do Kafka e para consumir da fila, exibindo em tela.

Disponibilizado uma interface, onde é possível decidir ser um Producer ou Consumer.

# Execução do ambiente
É mandatório ter o kafka rodando na porta 29092 e o Zookeeper na porta 22181 ou rodar o comando `docker compose up`, na pasta onde o arquivo docker-compose.yml está presente.

Assim a máquina subirá um servidor Kafka na porta 29092 e você poderá acessar o admin via  [Conduktor](https://conduktor.io/download "Conduktor").

# Importante
É necessário ter um tópico chamado **meu-topico**, você pode criá-lo a partir do Conduktor.

Divirta-se!!
