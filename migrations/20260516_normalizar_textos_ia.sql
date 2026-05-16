-- Normaliza textos de alertas de IA criados antes da revisão de português.

update public.alertas_ia
set
    titulo = replace(
        replace(
            replace(
                replace(titulo, 'Rubrica critica', 'Rubrica crítica'),
                'Cotacao atrasada',
                'Cotação atrasada'
            ),
            'Pendencia de auditoria',
            'Pendência de auditoria'
        ),
        'solicitacao',
        'solicitação'
    ),
    descricao = replace(
        replace(
            replace(
                replace(
                    replace(
                        replace(
                            replace(
                                replace(descricao, 'sem cotacao', 'sem cotação'),
                                'sem patrimonio',
                                'sem patrimônio'
                            ),
                            'cotacao vencedora',
                            'cotação vencedora'
                        ),
                        'ja comprometeu',
                        'já comprometeu'
                    ),
                    'orcamento',
                    'orçamento'
                ),
                'solicitacao',
                'solicitação'
            ),
            'esta parada',
            'está parada'
        ),
        ' ha ',
        ' há '
    ),
    sugestao_acao = replace(
        replace(
            replace(
                replace(sugestao_acao, 'pendencia', 'pendência'),
                'cotacao',
                'cotação'
            ),
            'solicitacoes',
            'solicitações'
        ),
        'patrimonio',
        'patrimônio'
    );
