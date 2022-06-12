A empresa solicitou o desenvolvimento de soluções MapReduce para extração das seguintes informações:

    1. País com a maior quantidade de transações comerciais efetuadas.
    2. Mercadoria com a maior quantidade de transações comerciais no Brasil (como a base de dados está em inglês, utilize Brazil).
    3. Quantidade de transações comerciais realizadas por ano.
    4. Mercadoria com maior quantidade de transações financeiras.    
    5. Mercadoria com maior quantidade de transações financeiras em 2016.
    6. Mercadoria com maior quantidade de transações financeiras em 2016, no Brasil (como a base de dados está em inglês, utilize Brazil).
    7. Mercadoria com maior total de peso, de acordo com todas as transações comerciais.
    8. Mercadoria com maior total de peso, de acordo com todas as transações comerciais, separadas por ano.


Para cada informação, crie uma classe correspondente no seu projeto, no seguinte formato:

    1. Informação 1 deverá ser implementada em uma classe denominada Informacao1.java.
    2. Informação 2 deverá ser implementada em uma classe denominada Informacao2.java.
    3. Informação 3 deverá ser implementada em uma classe denominada Informacao3.java.
    4. Informação 4 deverá ser implementada em uma classe denominada Informacao4.java.
    5. Informação 5 deverá ser implementada em uma classe denominada Informacao5.java.
    6. Informação 6 deverá ser implementada em uma classe denominada Informacao6.java.
    7. Informação 7 deverá ser implementada em uma classe denominada Informacao7.java.
    8. Informação 8 deverá ser implementada em uma classe denominada Informacao8.java.

Finalmente, após a implementação das oito soluções MapReduce para extração das informações solicitadas pela empresa, você irá executá-las no Hadoop, para a obtenção da informação sobre a base de dados completa. Para cada informação solicitada pela empresa, você deverá:

    1. Submeter uma tarefa ao Hadoop para extração da informação sobre a base completa armazenada no HDFS.
    2. Copiar o resultado do HDFS para o diretório local.
    3. Analisar o resultado.