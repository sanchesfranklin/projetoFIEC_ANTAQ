/*
Questão D

Dados do Ceará, Nordeste e Brasil 
contendo número de atracações, para cada localidade, bem como tempo de espera para atracar 
e tempo atracado por meses nos anos de 2020 e 20121.

*/

-- Cria uma tabela temporária

DECLARE @InfoAtracacoesT TABLE(
    localidade          NVARCHAR(100),
    numeroAtracacoes    INT,
    tempoEsperaMedio    TIME(0),
    tempoAtracadoMedio  TIME(0),
    mes                 INT,
    ano                 INT
)

-- Inserir dados tabela temporária

INSERT INTO @InfoAtracacoesT

    -- Ceará
    SELECT
        SGUF                          AS localidade,
        COUNT(IDAtracacao)            AS numeroAtracacoes,
        AVG(TEsperaAtracacao)         AS tempoEsperaMedio,
        mes                           AS mes,
        ano                           AS ano        
        AVG(atracacao.TAtracado)      AS tempoAtracadoMedio
    FROM atracacao_fato
    WHERE SGUF = 'CE'

    UNION ALL

    -- NORDESTE
    SELECT
        SGUF                          AS localidade,
        COUNT(IDAtracacao)            AS numeroAtracacoes,
        AVG(TEsperaAtracacao)         AS tempoEsperaMedio,
        mes                           AS mes,
        ano                           AS ano        
        AVG(atracacao.TAtracado)      AS tempoAtracadoMedio
    FROM atracacao_fato
    WHERE SGUF = 'Nordeste'

    -- Brasil
    SELECT
        'Brasil'                          AS localidade,
        COUNT(IDAtracacao)            AS numeroAtracacoes,
        AVG(TEsperaAtracacao)         AS tempoEsperaMedio,
        mes                           AS mes,
        ano                           AS ano        
        AVG(atracacao.TAtracado)      AS tempoAtracadoMedio
    FROM atracacao_fato
    WHERE SGUF IN (
        'AC', 'AL', 'AP', 'AM', 'BA','CE', 'ES', 'GO', 'MA', 'MT',
        'MS', 'MG', 'PA', 'PB', 'PR', 'PI', 'RJ', 'RN', 'RS',
        'RO', 'RR', 'SC', 'SP', 'SE', 'TO', 'DF', 'PE'
    )


-- RETORNANDO OS DADOS

SELECT
    *
FROM @InfoAtracacoesT
WHERE ano = 2020
    AND ano = 2021
