AUTOR:
Laís Nuto Rossman, NºUSP 12547274, laisnuto@usp.br


DESCRIÇÃO:
Este trabalho consiste na implementação de um servidor AMQP na versão 0.9.1 do protocolo em C. 
O servidor utiliza threads para lidar com múltiplos clientes simultaneamente. 
As mensagens são armazenadas em filas e os consumidores podem se registrar para consumir mensagens dessas filas.


COMO EXECUTAR:
Primeiramente, para compilar `Makefile` incluído no projeto basta rodar no terminal o comando: make
Depois para executar o programa, você pode iniciar o servidor utilizando o seguinte comando: ./amqp 5672
5672 indica a porta que o servidor estará rodando


VISÃO GERAL:
Conexões com o Servidor
Após iniciar, o servidor irá escutar por conexões na porta especificada na linha de comando da execução do código. 
O protocolo de comunicação segue o padrão AMQP e começa com um handshaking que inclui a negociação da versão do protocolo, configuração da conexão, abertura da conexão e abertura do canal.
Uma vez que a conexão esteja estabelecida, o cliente pode:
- Declarar uma fila: Para criar uma fila nova.
- Publicar uma mensagem: Envia uma mensagem para uma fila específica.
- Consumir uma mensagem: Indica que o cliente deseja consumir mensagens de uma fila específica.


TESTES:
Os testes do programa foram feitos em 3 situações: 0 clientes conectados, 10 clientes conectados e 100 clientes conectados.
Foi feito um script para fazer os comandos de forma automatizada e capturar os dados de uso da rede e da CPU.
Cada situação foi testada 10 vezes e uma média dos resultados foi calculado para facilitar a análise.


OBSERVAÇÕES:
- O servidor foi projetado para ser multithreaded, permitindo múltiplos clientes conectados simultaneamente.
- Mutexes são usados para garantir que as operações em dados compartilhados sejam thread-safe.
- Da forma com que o servidor foi implementado, as filas são identificadas pelo nome. Então se um cliente declarar duas filas com o mesmo nome, o programa pode não funcionar como o esperado.

    

DEPENDÊNCIAS:
  - Processador: i5-1135G7 2.40GHz x86_64
  - Versão do gcc: gcc 9.4.0
  - Sistema Operacional: Ubuntu 20.04.4 LTS 
  - Shell: bash 5.0.17 
  - Bibliotecas: 
    - `pthread` para suporte a multithreading.