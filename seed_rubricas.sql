insert into rubricas (codigo, nome, tipo, valor_orcado, responsaveis) values
('MC-01','Estrutura e integração instrumental','material_consumo',6500.00,'ANTONIO/KEPLER/SEVERINO'),
('MC-02','Insumos laboratoriais (reagentes, eletrodos, membranas, EPIs etc.)','material_consumo',53700.00,'BRUNA/MAYARA'),
('MC-03','Materiais antichama e proteção térmica','material_consumo',4800.00,'ANTONIO/KEPLER/SEVERINO'),
('MC-04','Insumos para cromatógrafo a gás','material_consumo',11000.00,'ANTONIO/KEPLER/SEVERINO'),
('MC-05','Materiais filtrantes e purificação de gases','material_consumo',2700.00,'ANTONIO/KEPLER/SEVERINO'),
('MC-06','Componentes cerâmicos refratários','material_consumo',4500.00,'ANTONIO/KEPLER/SEVERINO'),
('MC-07','Sensores (temperatura, pressão, vazão, pH, etc.)','material_consumo',59200.00,'ANTONIO/KEPLER/SEVERINO'),
('MP-01','Fonte DC programável','material_permanente',25000.00,'KEPLER / ANTONIO / SEVERINO'),
('MP-02','Medidores de pressão','material_permanente',10500.00,'KEPLER / ANTONIO / SEVERINO'),
('MP-03','Sistema de medição de temperatura','material_permanente',12000.00,'KEPLER / ANTONIO / SEVERINO'),
('MP-04','Sistema de aquisição e automação de dados','material_permanente',45000.00,'KEPLER / ANTONIO / SEVERINO'),
('MP-05','Condutivímetro de bancada','material_permanente',13000.00,'BRUNA / CAROLINA / MAYARA'),
('MP-06','pHmetro','material_permanente',11000.00,'BRUNA / CAROLINA / MAYARA'),
('MP-07','Fluxômetros e acessórios','material_permanente',3800.00,'BRUNA / CAROLINA / MAYARA'),
('MP-08','Equipamentos auxiliares (bombas e sistema de vácuo)','material_permanente',8500.00,'KEPLER / ANTONIO / SEVERINO'),
('MP-09','Controladores de vazão de gases','material_permanente',24000.00,'KEPLER / ANTONIO / SEVERINO'),
('PF-01','Manutenção de cromatógrafo','servico_pf',15000.00,null)
on conflict (codigo) do update set valor_orcado=excluded.valor_orcado, responsaveis=excluded.responsaveis;
