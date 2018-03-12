# Primeiro Projeto de Sistemas Distribuídos

#Objectivo

O objetivo do trabalho é desenvolver um sistema de processamento distribuído de dados textuais, baseado no paradigma MapReduce.
Envolverá (1) implementar um repositório distribuído para armazenar os dados a processar e o código fonte Java dos programas MapReduce a executar; (2) adaptar uma solução MapReduce existente, centralizada, afim de tirar partido da distribuição.

#MapReduce (Paradigma)

Um programa MapReduce centra-se em duas funções: map e reduce. Usualmente, a função map é usada para transformar ou filtrar os dados de entrada, enquanto que a função reduce produz uma agregação dos dados intermédios produzidos pela função map.
Estas funções operam sobre tuplos (chave, valor) e emitem zero ou mais tuplos (chave, valor) do mesmo ou de outro tipo. A função map é alimentada com os tuplos de entrada, por uma ordem arbitrária. Porém, a cada invocação da função reduce são processados todos os valores dos tuplos com a mesma chave, emitidos pela função map.
Este modelo é compatível com uma execução distribuída (e paralela). Numa primeira fase, dados localizados em múltiplas máquinas podem ser processados localmente pela função map, em paralelo. Consequentemente, em cada máquina serão emitidos um número arbitrário de novos tuplos (chave, valor). Posteriormente, os tuplos emitidos na fase anterior que partilham a mesma chave devem ser reunidos e os seus valores entregues ao mesmo reducer, podendo chaves distintas ser executadas em paralelo (e em máquinas distintas). Note-se que o resultado da fase reduce poderá estar particionado, caso tenham sido produzidos por mais do que uma instância reduce.
Quando a computação desejada é demasiado complexa para realizar num só passo MapReduce, é possível encadear programas MapReduce, usando os resultados do passo anterior para alimentar o seguinte.

#Componentes do Sistema

-Repositório

O repositório de dados está limitado a dados textuais. A unidade de armazenamento é o blob de texto. Um blob é caracterizado por um nome e é composto por um número arbitrário (potencialmente muito grande) de linhas de texto.
Os nomes dos blobs podem ter um comprimento variável, mas o espaço de nomes é plano, ie., sem hierarquia explícita.
Além de permitir ler e escrever um blob particular, dado o nome, o repositório permite listar (ou apagar), de uma só vez, todos os blobs cujos nomes têm um determinado prefixo.
Devido à dimensão arbitraria (potencialmente muito grande) dos blobs, estes serão armazenados em blocos de texto. Os blocos de texto terão uma dimensão máxima (em bytes) limitada, garantindo-se ainda que os blocos armazenam linhas de texto completas. Portanto, a dimensão máxima das linhas que é possível armazenar no repositório está limitada pela dimensão máxima do bloco permitida.

-Arquitectura

O respositório compreende dois tipos de componentes, um Namenode e vários Datanodes. Os Datanodes têm como função armazenar blocos de texto, correspondentes ao conteúdo dos blobs. O Namenode serve para guardar o nome dos blobs existentes no repositório e quais os blocos que os compõem. Um blob poderá ter o seu conteúdo espalhado por vários Datanodes.

-Motor MapReduce

O motor MapReduce opera exclusivamente sobre blobs armazenados no repositório. Os dados de entrada dos programas MapReduce a executar consistem na informação contida nos blobs, cujos nomes partilham um dado prefixo. Os resultados da execução serão também escritos no repositório, resultando em zero ou mais blobs com nomes gerados a partir do prefixo dado. Resultados intermédios (produzidos na fase map) podem ser temporariamente armazenados no repositório.
O código fonte Java dos programas MapReduce será também lido (e compilado) a partir de um blob previamente colocado no repositório.

-Execução Centralizada (dos Programas MapReduce)

Uma computação MapReduce tem como base um código fonte, escrito em Java, e guardado no repositório num blob. Os dados de entrada da computação serão obtidos do repositório e têm como alvo um número variável de blobs cujo nome contém um prefixo dado como parâmetro da computação. Eventuais dados de saída serão escritos num ou mais blobs, construindo o seu nome também com base num prefixo.

In [ ]:
void MapReduceEngine.execute( String jobClassBlob, String inputPrefix , String outputPrefix );

A versão centralizada do motor de execução fornecido caracteriza-se por correr integralmente no cliente que puxa os dados integralmente para executar os programas MapReduce. Esta versão tem como objetivo descrever as fases de execução MapReduce e como o repositório é explorado para esse fim. Nomeadamente, ilustra como se tira partido da possibilidade dada pelo repositório de agregar nomes de blobs com base num prefixo.
Quando é lançada uma execução, começa pela fase map. Uma única tarefa com esse fim irá iterar sobre todas as linhas de todos os blobs cujo nome contém o prefixo indica como parâmetro da execução. Para cada uma dessas linha é invocada a função map do programa MapReduce (este é compilado nesse momento). Esta função irá gerar tuplos (chave, valor), tipados de acordo com o código do programa. Cada chave irá traduzir-se na criação de um novo blob, visando recolher os valores dos tuplos emitidos que partilhem essa chave. O nome do blob gerado para cada chave tem o seguinte formato:
<output-prefix>-map-<key>-<worker>
O valor de <key> resulta de duas codificações. Primeiro, o valor da chave é convertido para JSON e esse resultado é codificado em Base58. Desta maneira, <key> será composto apenas por letras e números. O sufixo <worker> na versão centralizada toma o valor de "client" mas é livre, desde que resulte num nome válido (para o repositório).
Terminada a fase map, terão sido produzidos e guardados no repositório tantos blobs, quantas as chaves que foram emitidas. Esses blobs podem ser enumerados interrogando o repositório pelos nomes dos blobs com o prefixo: <output-prefix>-map-
Segue-se a fase reduce. Para cada chave, antes de poder invocar a operação reduce, é necessário selecionar todos os valores que a partilham. As chaves a processar são determinadas por via dos nomes dos blobs produzidos na fase map. Correspondem ao valor tomado por <key> nos nomes dos blobs relevantes. Para cada chave e os respetivos valores é invocada a função reduce do programa. Os resultados da função reduce são recolhidos em blobs cujo nome tem o seguinte formato:
<output-prefix>-reduce-<key>-<worker> ou <output-prefix>-reduce-<partition>-<worker>, dependendo da configuração do motor.
O primeiro caso, corresponde a emitir um blob com os resultados da função reduce para cada chave individual. No segundo caso, o conjunto das chaves que serão alvo da função reduce é dividido em partições (disjuntas) e processam-se as chaves de cada partição sequencialmente, indo os resultados de cada partição para um mesmo blob.
Sendo uma versão centralizada, o motor de execução está aquém do possível. explora o facto de a leitura e escrita de blobs com nomes distintos poder fazer-se em paralelo.


#Requisitos da Solução

A solução a desenvolver deve implementar um conjunto de funcionalidades base conformes com a especificação do sistema presente neste enunciado. A título valorativo, poderão ser implementadas outras funcionalidades.

-Funcionalidades Base

Serviços Namenode & Datanode implementados usando tecnologia REST Jersey JAX-RS; €€€
Estado dos Datanodes persistente em memória secundária local (disco); €
Biblioteca cliente funcional para acesso a um repositório remoto constituído por um Namenode e vários Datanodes; €€
Tolerar falhas de comunicação temporárias; €
Auto-configuração; €
Dado um grupo IP multicast e porto, pré-combinados, os componentes deverão responder a uma mensagem contendo o seu nome lógico ("Namenode" ou "Datanode") com o respetivo URI. Com este mecanismo, suportar a auto-organização do repositório e descoberta do repositório pelos clientes essa camada.
Nota: Cumprindo os requisitos acima, o motor MapReduce fornecido deverá poder operar com um repositório remoto (ainda que a computação se faça centralizadamente no cliente)

-Funcionalidades Opcionais (Valorativas)

Motor MapReduce descentralizado. €€€
Suporte da execução remota (e distribuída) do motor MapReduce, conforme a interface WebServices SOAP indicada abaixo. €€
Explorar a afinidade entre dados e as tarefas MapReduce (localidade dos dados) €€€
Garbage Collection. €
Remoção de blocos não alcançáveis, não pertencentes a nenhum blob presente no Namenode.
