# Usuários & Roles – Controle de Acesso

O controle de acesso ao banco de dados PostgreSQL é estruturado para garantir **segurança**, **governança** e rastreabilidade.  
Abaixo estão descritos os principais usuários, roles e as permissões concedidas para cada um.

---

## Estrutura de Roles

| Role          | Descrição                                 | Permissões                                     |
|---------------|-------------------------------------------|------------------------------------------------|
| `bi_readonly` | Role de leitura para BI/relatórios        | SELECT nas tabelas dos schemas dim, fato, link |

---

## Usuários

| Usuário         | Pertence à Role     | Pode Logar | Observação                             |
|-----------------|--------------------|:----------:|----------------------------------------|
| `powerbi_user`  | bi_readonly        | ✅         | Usuário dedicado para conexão Power BI |
| `etl_user`      | etl (exemplo)      | ✅         | Usuário dedicado para rotinas ETL      |
| `postgres`      | -                  | ✅         | Superusuário, uso restrito ao DBA      |

---

## Permissões Concedidas

- O usuário **`powerbi_user`** pode realizar consultas de leitura (`SELECT`) nas tabelas dos schemas:
  - `dim`
  - `fato`
  - `link`
- Não possui permissões de escrita, alteração ou exclusão.
- Permissão garantida também para futuras tabelas criadas nesses schemas.

---

!!! note
    Para a criação de novos usuários, revisão ou remoção de permissões, consulte sempre o DBA responsável e siga as políticas de segurança da organização.