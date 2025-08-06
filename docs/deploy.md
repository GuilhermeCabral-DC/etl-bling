Arquitetura dos Scripts
Esta seção descreve a estrutura modular do código Python, detalhando a função de cada arquivo e como eles se integram para compor um pipeline ETL robusto, escalável e de fácil manutenção.

Organização dos Módulos
O projeto é dividido em módulos independentes, cada um com responsabilidades bem definidas, seguindo boas práticas de separação de responsabilidades:

Módulo	Descrição
main.py	Orquestração central do pipeline ETL: inicia e monitora as cargas
bling_api.py	Camada de integração: realiza requisições à API do Bling
etl.py	Lógica de controle incremental e execução das rotinas de carga
db.py	Operações com PostgreSQL: conexões, inserts, upserts e comandos SQL
transformers.py	Transformações, validações e padronização dos dados extraídos
log.py	Logging centralizado e estruturado (execução, falhas, métricas)
auth.py	Gerenciamento da autenticação OAuth2 com a API Bling
config.py	Carrega e centraliza variáveis de ambiente e parâmetros globais
utils.py	Funções utilitárias diversas (helpers)
obter_token.py	Script manual para obtenção ou renovação de tokens OAuth2
date_utils.py	Funções específicas para manipulação e padronização de datas

Fluxo Geral do Pipeline
Abaixo um resumo textual do fluxo principal do pipeline:

O main.py inicia a execução do processo ETL.

O módulo auth.py gerencia a autenticação e renovação dos tokens de acesso.

O bling_api.py executa as chamadas aos endpoints do Bling, recebendo os dados brutos.

O etl.py controla o processamento incremental, definindo quais entidades e períodos devem ser processados.

O transformers.py normaliza e valida os dados extraídos.

O db.py realiza a inserção ou atualização dos dados no PostgreSQL, utilizando operações otimizadas para grandes volumes.

O log.py registra logs estruturados sobre cada etapa, incluindo erros, execuções e métricas de performance.

Outros módulos de suporte, como utils.py, config.py e date_utils.py, são utilizados sempre que funções utilitárias ou parâmetros globais são necessários.

Boas Práticas Adotadas
Separação de responsabilidades: Cada módulo cuida de uma etapa ou serviço específico.

Reaproveitamento de código: Funções utilitárias centralizadas em módulos próprios.

Escalabilidade: Suporte para novas entidades/endpoints e fácil expansão do pipeline.

Controle e rastreabilidade: Logging centralizado e tabelas de controle no banco garantem rastreamento completo das execuções.

Para detalhes do código de cada módulo, consulte a documentação detalhada na seção correspondente do menu lateral.