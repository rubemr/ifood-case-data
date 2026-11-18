# Exploração de janeiro de 2023

E06 executado com PySpark 4.2.0 sobre **3.066.766 corridas**, snapshot Delta 1 de `workspace.ifood_bronze.yellow_trips`. O recorte é o **arquivo de origem janeiro**, salvo quando indicado como mês-calendário. A Bronze foi preservada; nenhuma linha foi excluída, corrigida ou deduplicada.


## Qualidade da fonte

| Achado | Corridas | Interpretação / decisão proposta |
|---|---:|---|
| passenger_count nulo | 71.743 | Informação de ocupação ausente; não imputar |
| passenger_count zero | 51.164 | Manter valor e flag; comparar médias com e sem zero |
| passenger_count negativo ou fracionário | 0 | Tipo inteiro é compatível com os valores observados |
| total_amount negativo | 25.204 | Preservar e sinalizar; não assumir erro ou estorno sem confirmação |
| total_amount zero | 568 | Preservar; não excluir automaticamente |
| Pickups fora de janeiro | 48 | Atribuir competência analítica pelo pickup, não pelo nome do arquivo |
| Pickups fora de jan–mai/2023 | 38 | Preservar na Silver; fora da população das métricas do case |
| Dropoff anterior ao pickup | 3 | Flag de duração inválida; não afeta automaticamente métricas que só dependem de pickup |
| Duração igual a zero segundos | 1.118 | Sinalizar, sem exclusão automática |
| Duração superior a 24 horas | 25 | Diagnóstico de valores extremos; 24h não é limite de exclusão aprovado |

VendorID, total_amount, pickup e dropoff não apresentaram nulos. Não foram encontrados valores não finitos, falhas de cast nas duas medidas ou alterações monetárias superiores a 1e-9 no cast para DECIMAL(18,2). Isso descreve janeiro; repetir as verificações nas outras fontes antes de generalizar.

Passageiros observados: de 0 a 9; valores de 7–9 aparecem em 20 registros. Não limitar a seis passageiros sem regra de negócio. Valor total: de USD -751,00 a USD 1.169,40. Percentis aproximados p50/p95/p99 de total_amount: USD 20,16 / 80,25 / 101,94. Durações positivas: p50/p95/p99 de 692 / 2.190 / 3.438 segundos. Percentis foram calculados com `percentile_approx`, precisão 10.000.

## Datas e competência

| Mês do pickup no arquivo de janeiro | Corridas |
|---|---:|
| 2008-12 | 2 |
| 2022-10 | 11 |
| 2022-12 | 25 |
| 2023-01 | 3.066.718 |
| 2023-02 | 10 |

Os 10 pickups de fevereiro ficam no período do case, com competência fevereiro. Os outros 38 ficam fora da janela. Não corrigir o ano nem forçar a data para janeiro: isso inventaria informação. Como o case especifica somente os cinco arquivos, a análise deve declarar sua cobertura e não prometer completude de todos os eventos de fronteira presentes em arquivos adjacentes não solicitados.

## Sensibilidade das médias — somente pickups de janeiro

| Regra de passageiros | Corridas no denominador | Média por corrida |
|---|---:|---:|
| Inteiros positivos, sem nulos e sem zeros | 2.943.811 | 1,386215 |
| Inteiros não negativos, sem nulos e incluindo zeros | 2.994.975 | 1,362534 |

Excluir zeros aumenta a média em aproximadamente **1,74%** em relação à versão que os inclui. A leitura publicada usa **1,362534 com zeros reportados** e **1,386215 apenas com positivos na comparação**. O significado dos zeros não foi confirmado; nenhuma leitura é declarada estimativa sem viés da ocupação real. Não substituir nulos por zero. Essa comparação é de janeiro e **não responde à pergunta de maio por hora**.

## Sensibilidade financeira — somente pickups de janeiro

| Regra | Soma de total_amount (USD) |
|---|---:|
| Incluindo valores negativos | 82.863.379,14 |
| Excluindo valores negativos | 83.468.375,98 |
| Contribuição dos 25.204 valores negativos | -604.996,84 |

Excluir negativos aumentaria o total reportado em aproximadamente **0,73%**. A proposta preserva negativos na métrica principal e mostra a sensibilidade. Não interpretar a soma como lucro nem como recebimento líquido. Este total é da fonte janeiro com pickup janeiro; o relatório final usa os cinco arquivos. Não confundir as populações.

## Duplicatas aparentes

Foram encontrados **36 grupos**, envolvendo **72 linhas**, iguais nas cinco colunas obrigatórias. Deduplicar por esses campos eliminaria 36 linhas. Como não há chave única de corrida nessas colunas, os grupos são apenas aparentes; podem diferir em outras colunas ou representar corridas reportadas distintas. Nenhuma linha foi removida e isso não comprova 36 duplicatas em todas as 19 colunas.

## Reconciliação das regras propostas

Sobre todo o arquivo, usando a janela jan–mai, o relatório executado avaliou estas populações exploratórias:

- Financeiro: 3.066.728 elegíveis + 38 não elegíveis = 3.066.766.
- Passageiros positivos: 2.943.821 elegíveis + 122.945 não elegíveis = 3.066.766.

O contrato atual da Silver inclui passageiros zero: na janela do case, a população não negativa do arquivo de janeiro é 2.994.985 (2.943.821 positivos mais 51.164 zeros). A comparação acima com somente positivos mantém seu próprio denominador.

Os motivos de anomalia podem se sobrepor; não somá-los como rejeições exclusivas. As contagens de nulos, zeros, valores negativos, datas fora das janelas e duração negativa/zero também foram recalculadas com PyArrow sobre a fonte original e coincidiram com Spark.


## Revisão de 17/11/2026

A revisão repetiu as contagens e não encontrou strings vazias nos campos obrigatórios. Todos os 71.743 passageiros NULL estão em payment_type=0; 50.988 dos 51.164 zeros estão no VendorID 1. O agrupamento é descritivo e não identifica o mecanismo de ausência. As médias descrevem valores reportados, não ocupação real comprovada da frota. A política de qualidade e as referências estão no [README](../README.md).
