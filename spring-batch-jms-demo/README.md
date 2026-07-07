# Spring Batch JMS Remote Partitioning Demo

Este projecto demonstra uma arquitetura robusta de **Spring Batch distribuído (Remote Partitioning) via JMS**.
A configuração foi desenhada para resolver dois problemas críticos em instâncias homogéneas (onde qualquer nó pode ser *Master* ou *Worker*):

1. **O Risco de Isolamento (O "Roubo" de Mensagens):** Evitar que o orquestrador (Master) de uma máquina processe as respostas de partições de um Job lançado noutra máquina. Resolvido omitindo o canal de resposta estático no lado do Manager e recorrendo a *Temporary Queues* injetadas pelo cabeçalho `JMSReplyTo`.
2. **A Falha do Trabalhador (Risco de Duplicação Sem XA):** Evitar que dados sejam duplicados na base de dados se um *Worker* for abaixo (kill -9) a meio do processamento. Resolvido com *Best Efforts 1 Phase Commit* (1PC), injetando o Transaction Manager da DB no Listener do JMS (as mensagens só recebem *Acknowledge* na fila após o commit na BD).

---

## Pré-Requisitos
* Java 21.
* Maven 3.x.
* Terminal (Linux/Mac/PowerShell/Cmd).

---

## Estrutura do Projecto

A infraestrutura é simétrica. Todos os nós carregam os mesmos componentes e escutam a mesma fila de pedidos (`batch.partition.requests.queue`).

Para facilitar os testes sem dependências externas, configuramos:
* **H2 em modo File/Server (`AUTO_SERVER=TRUE`)**: Partilhada entre todas as JVMs arrancadas localmente.
* **ActiveMQ embutido no Node 1**: A primeira JVM (`start-node1`) levanta um Broker TCP (porto `61616`) ao qual as outras instâncias se conectam.

### Scripts Disponibilizados (Existem as versões `.bat` e `.sh`):
* `start-node1` : Inicia a App no porto 8080 e levanta o Broker ActiveMQ TCP. **Deve ser sempre o primeiro a arrancar.**
* `start-node2` : Inicia a App no porto 8081. Liga-se ao ActiveMQ do Node 1.
* `start-node3` : Inicia a App no porto 8082. Liga-se ao ActiveMQ do Node 1.
* `trigger-job-node1` : Envia um pedido POST (`/run`) à API da Máquina 1, transformando-a no Orquestrador.
* `trigger-job-node2` : Envia um pedido POST (`/run`) à API da Máquina 2, transformando-a no Orquestrador.

---

## Testes Automatizados (Integração)

Foi implementada uma class de teste (`SpringBatchDistributedIT.java`) que cobre o caminho feliz da arquitetura:
A class usa JUnit 5 e Awaitility para lidar com a natureza assíncrona do JMS.
Devido à partilha de estado na mesma JVM nestes testes, as falhas catastróficas (kill -9) são mais facilmente observadas usando os scripts para simular máquinas separadas.
A arquitetura do código garante a recuperação através do `TransactionManager` da BD associado à escuta de mensagens.

Para correr os testes:
```bash
mvn clean test
```

---

## Como simular os Test Cases Manuais

*Nota: O `ItemProcessor` tem um pequeno `Thread.sleep(1000)` para que tenhas tempo de efetuar os testes de falha (kill).*

### 1. Caminho Feliz e Elasticidade (Happy Path & Elasticity)

Testar se o trabalho é distribuído corretamente e se a infraestrutura se adapta dinamicamente.

**TC 1.1: Arrancar o job na Maq 1, com Maq 1 e Maq 2 ligadas desde o início.**
1. Abre 2 terminais.
2. Executa `./start-node1.sh` num terminal e `./start-node2.sh` noutro.
3. Executa `./trigger-job-node1.sh`.
* **Resultado:** O job termina com sucesso. Os logs mostram o processamento a ser distribuído entre a Máquina 1 e a Máquina 2. O Orquestrador aguardará de forma assíncrona até todas as tarefas das Máquinas devolverem COMPLETED via Temporary Queue.

**TC 1.3: Arrancar o job na Maq 1, apenas com a Maq 1 ligada.**
1. Desliga o Node 2. Garante que só o Node 1 está a correr.
2. Executa `./trigger-job-node1.sh`.
* **Resultado:** O job demora mais tempo, mas a Máquina 1 processa as partições sozinha com sucesso.

**TC 1.4: Arrancar o job na Maq 1 (com Maq 2 desligada). A meio do processamento, ligar a Maq 2.**
1. Só o Node 1 a correr. Arranca o job: `./trigger-job-node1.sh`.
2. Assim que começar a processar, abre outro terminal e corre `./start-node2.sh`.
* **Resultado:** A Máquina 2 liga-se à fila JMS e começa imediatamente a "roubar" itens (partições) à Máquina 1, acelerando o processamento. O job termina com sucesso.

---

### 2. Resiliência de Slaves (Falhas de Workers)

**TC 2.1: Arrancar o job na Maq 1. A meio do processamento, forçar a paragem (kill -9) da Maq 2.**
1. Arranca Node 1 e Node 2.
2. Arranca o job no Node 1: `./trigger-job-node1.sh`.
3. Quando vires os logs de processamento no Node 2, faz "Ctrl+C" no terminal do Node 2 (ou usa `kill -9` no PID associado ao port 8081).
* **Resultado Esperado (Graças ao 1PC configurado):** A ligação à base de dados no Node 2 morre (fazendo rollback na transação em curso) e o consumo da mensagem JMS também é revertido visto não enviar o Acknowledge. A mensagem da partição afetada volta à fila principal. O Node 1 (que continua em pé) vai consumir de imediato essa mensagem e terminar o processamento sem que existam dados duplicados de negócios inseridos na DB.

---

### 3. Falhas do Orquestrador (O Teste de Fogo)

**TC 3.1: Arrancar o job na Maq 2. A meio do processamento, forçar a paragem (kill -9) da Maq 2 (Orquestrador).**
1. Arranca Node 1 e Node 2. (Garante que o broker no Node 1 se mantém ativo)
2. Arranca o job no Node 2: `./trigger-job-node2.sh`.
3. O Node 2 transforma-se em Master.
4. Quando iniciar o processamento, mata o processo do Node 2 (`kill -9`).
* **Resultado:** A fila temporária criada pelo Node 2 (`JMSReplyTo`) é destruída mal a sua ligação ActiveMQ cai. O Node 1 vai processar e completar as partições atribuídas a si. Quando as tentar devolver ao Node 2, a fila de resposta já não existirá. O Job Repository irá marcar as `StepExecution` dos Workers como finalizadas, mas o `JobExecution` geral ficará estático em `STARTED`. Em produção, poderias invocar manualmente um recomeço desse job ou limpar o estado falhado, sabendo garantidamente que o que estava concluído não repetiria.

---

### 4. Isolamento e Concorrência (Prevenir "Roubo" de Mensagens)

**TC 4.1: Lançamento Concorrente. Arrancar o Job A na Maq 1 e o Job B na Maq 2.**
1. Arranca Node 1 e Node 2.
2. Numa janela de terminal à parte, executa `./trigger-job-node1.sh & ./trigger-job-node2.sh` rapidamente.
* **Resultado Esperado (Graças às Temporary Queues):** Os jobs correm em paralelo. As respostas do Job da Máquina 1 vão *exclusivamente* para a Fila Temporária da Máquina 1, e as respostas do Job da Máquina 2 vão exclusivamente para a Fila Temporária da Máquina 2. Nenhum "Master" vai consumir a mensagem destinada ao "Master" da outra máquina. Ambas execuções concluirão os seus percursos de modo isolado.
