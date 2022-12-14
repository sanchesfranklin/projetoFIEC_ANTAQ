---------------------------
-- CRIA DATABASE CASO AINDA NÃO EXISTA
CREATE DATABASE antaqFiecDb

USE antaqFiecDb
GO

---------------------------
-- CRIA A TABELA ATRACACAO_FATO CASO AINDA NÃO EXISTA

IF (NOT EXISTS (
    SELECT * 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'dbo' 
        AND  TABLE_NAME = 'atracacao_fato')
    )
BEGIN
    CREATE TABLE atracacao_fato(
        IDAtracacao                 BIGINT                 NOT NULL        PRIMARY KEY,
        CDTUP                       NVARCHAR(100),
        IDBerco                     NVARCHAR(100),
        Berco                       NVARCHAR(100),
        PortoAtracacao              NVARCHAR(100),
        ApelidoInstalacaoPortuaria  NVARCHAR(100),
        ComplexoPortuario           NVARCHAR(100),
        TipoAutoridadePortuaria     NVARCHAR(100),
        DataAtracacao               DATETIME,
        DataChegada                 DATETIME,
        DataDesatracacao            DATETIME,
        DataInicioOperacao          DATETIME,
        DataTerminoOperacao         DATETIME,
        Ano                         INT,
        Mes                         INT,
        TipoOperacao                NVARCHAR(100),
        TipoNavegacaoAtracacao      NVARCHAR(100),
        NacionalidadeArmador        NVARCHAR(100),
        FlagMCOperacaoAtracacao     NVARCHAR(100),
        Terminal                    NVARCHAR(100),
        Municipio                   NVARCHAR(100),
        UF                          NVARCHAR(100),
        SGUF                        NVARCHAR(100),
        RegiaoGeografica            NVARCHAR(100),
        NumeroCapitania             NVARCHAR(100),
        NumeroIMO                   NVARCHAR(100),
        TEsperaAtracacao            TIME(0),
        TEsperaInicioOp             TIME(0),
        TOperacao                   TIME(0),
        TEsperaDesatracacao         TIME(0),
        TAtracado                   TIME(0),
        TEstadia                    TIME(0)
    )
END



---------------------------
-- CRIA A TABELA CARGA_FATO CASO AINDA NÃO EXISTA
IF (NOT EXISTS (
    SELECT * 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'dbo' 
        AND  TABLE_NAME = 'carga_fato')
    )
BEGIN
    CREATE TABLE carga_fato(
        IDCarga                             BIGINT              NOT NULL      PRIMARY KEY,
        IDAtracacao                         BIGINT,
        Origem                              NVARCHAR(100),
        Destino                             NVARCHAR(100),
        CDMercadoria                        NVARCHAR(100),
        TipoOperacaoCarga                   NVARCHAR(100),
        CargaGeralAcondicionamento          NVARCHAR(100),
        ConteinerEstado                     NVARCHAR(100),
        TipoNavegacao                       NVARCHAR(100),
        FlagAutorizacao                     NVARCHAR(100),
        FlagCabotagem                       NVARCHAR(100),
        FlagCabotagemMovimentacao           NVARCHAR(100),
        FlagConteinerTamanho                NVARCHAR(100),
        FlagLongoCurso                      NVARCHAR(100),
        FlagMCOperacaoCarga                 NVARCHAR(100),
        FlagOffshore                        NVARCHAR(100),
        FlagTransporteViaInterioir          NVARCHAR(100),
        PercursoTransporteViasInteriores    NVARCHAR(100),
        PercursoTransporteInteriores        NVARCHAR(100),
        STNaturezaCarga                     NVARCHAR(100),
        STSH2                               NVARCHAR(100),
        STSH4                               NVARCHAR(100),
        NaturezaCarga                       NVARCHAR(100),
        Sentido                             NVARCHAR(100),
        TEU                                 NVARCHAR(100),
        VLPesoCargaBruta                    NVARCHAR(100),
        QTCarga                             INT,
        PesoLiquidoCarga                    INT
    )

    
    -- Criando relacionamento entre as tabelas
    ALTER TABLE dbo.carga_fato
    ADD FOREIGN KEY (IDAtracacao) REFERENCES dbo.atracacao_fato(IDAtracacao)
END
GO

