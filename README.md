# OSTicket Data Pipeline – Databricks


Este projeto implementa um pipeline de dados, utilizando o Databricks Free Edition,
seguindo a arquitetura Bronze, Silver e Gold.

## 🎯 Objetivo

Implementar um pipeline de engenharia de dados utilizando dados dos
chamados do TechOps, garantindo o processamento e métricas prontas para consumo analítico.

## 🔎 Sobre o OSTicket

OSTicket é uma ferramenta de Service Desk e gerenciamento de chamados de código aberto.

Neste projeto, o OSTicket foi utilizado como fonte de dados dos tickets de suporte (TechOps), permitindo a construção de métricas,
acompanhamento de SLA e análises históricas de desempenho.

_Os dados usado do OSTicket neste projeto são: ost_thread_entry.csv, ost_ticket.csv, ost_staff.csv, ost_ticket_cdata.csv, ost_ticket_status.csv_

## 🏗️ Arquitetura de Dados
O pipeline segue o padrão:

Raw → Bronze → Silver → Gold

- **Raw**: Armazenamento dos dados brutos (rastreamento e reprocessamento)
- **Bronze**: ingestão dos dados brutos
- **Silver**: tratamento, limpeza e transformação
- **Gold**: agregações e métricas para consumo analítico

A execução é controlada por um notebook orquestrador, que garante a ordem correta entre as camadas.


## 🧰 Tecnologias Utilizadas
- Databricks Free Edition
- Apache Spark (PySpark / Spark SQL)
- Delta Lake
- GitHub (versionamento)

## 📁 Estrutura do Projeto

```
|── pipelines/
    ├── bronze/
    ├── silver/
    ├── gold/
```

## ▶️ Como Executar o Pipeline

### Execução no Databricks

Este projeto é desenvolvido e testado utilizando notebooks do Databricks,
permitindo iteração rápida, validação visual dos dados e depuração durante o desenvolvimento.

Para fins de versionamento e melhor legibilidade no GitHub,
os notebooks são convertidos para arquivos `.py` (Source File),
que representam o snapshot do código em seu estado estável.

A execução do pipeline ocorre exclusivamente via notebooks no Databricks.

### Passo a passo

1. Acesse o **Databricks Free Edition**
2. Abra o notebook `setup` e execute utilizando a opção **Run All** para criar o catalogo e os volumes
3. Certifique-se de que os dados brutos estejam disponíveis no volume **Raw**
4. Abra o notebook `orquestrador`
5. Execute o notebook utilizando a opção **Run All**

O pipeline será executado automaticamente na seguinte ordem:

**Bronze → Silver → Gold**
## 📊 Camada Gold e Métricas Analíticas
🔐 **Confidencialidade dos dados**

Por se tratar de um projeto baseado em dados reais de operações internas de TechOps, todos os valores numéricos sensíveis foram propositalmente ocultados ou anonimizados nos dashboards apresentados neste repositório.

A estrutura, métricas, indicadores, visualizações e regras de negócio permanecem fiéis ao cenário real, sendo o objetivo demonstrar modelagem de indicadores, design de dashboards e análise operacional, e não a exposição de dados confidenciais.****

As métricas da camada Gold foram definidas com foco em análise operacional e acompanhamento de SLA.

➡️ [Clique aqui para ver o detalhamento das métricas e dashboards no Power BI](docs/metricas_e_dashboard.md)



## 👤 Autor
Matheus Inhaia

