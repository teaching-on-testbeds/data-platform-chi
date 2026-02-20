
::: {.cell .markdown}

## Delete and release resources

At the end of the lab, we delete the Chameleon server and lease so we do not leave compute reserved.

We will execute the next cells in the Chameleon Jupyter environment.

:::

::: {.cell .code}
```python
# run in Chameleon Jupyter environment
from chi import server, lease, context
import chi, os

context.version = "1.0"
context.choose_project()
context.choose_site(default="KVM@TACC")
```
:::

::: {.cell .code}
```python
# run in Chameleon Jupyter environment
username = os.getenv("USER")
server_name = f"node-data-{username}"
lease_name = f"lease-data-{username}"
```
:::

::: {.cell .code}
```python
# run in Chameleon Jupyter environment
s = server.get_server(server_name)
server.delete_server(s.id)
```
:::

::: {.cell .code}
```python
# run in Chameleon Jupyter environment
l = lease.get_lease(lease_name)
lease.delete_lease(l.id)
```
:::
