
::: {.cell .markdown}

## Launch and set up a VM instance with python-chi

We will use the `python-chi` Python API to Chameleon to provision a VM instance.

We will execute the cells in this notebook inside the Chameleon Jupyter environment.

Run the following cell and make sure the correct project is selected.

:::

::: {.cell .code}
```python
# run in Chameleon Jupyter environment
from chi import server, context, lease, network
import chi, os, datetime

context.version = "1.0"
context.choose_project()
context.choose_site(default="KVM@TACC")
username = os.getenv("USER")  # all experiment resources will have this prefix
```
:::

::: {.cell .markdown}

We will bring up an `m1.xlarge` flavor server with the `CC-Ubuntu24.04` disk image.

> **Note**: the following cell brings up a server only if we do not already have one with the same name, regardless of its error state. If we already have a server in ERROR state, delete it first in the Horizon GUI before running this cell.

:::

::: {.cell .markdown}

First, we reserve the VM instance for 8 hours, starting now:

:::

::: {.cell .code}
```python
# run in Chameleon Jupyter environment
l = lease.Lease(f"lease-data-{username}", duration=datetime.timedelta(hours=8))
l.add_flavor_reservation(id=chi.server.get_flavor_id("m1.xlarge"), amount=1)
l.submit(idempotent=True)
```
:::

::: {.cell .code}
```python
# run in Chameleon Jupyter environment
l.show()
```
:::

::: {.cell .markdown}

Now we can launch an instance using that lease:

:::

::: {.cell .code}
```python
# run in Chameleon Jupyter environment
s = server.Server(
    f"node-data-{username}",
    image_name="CC-Ubuntu24.04",
    flavor_name=l.get_reserved_flavors()[0].name,
)
s.submit(idempotent=True)
```
:::

::: {.cell .markdown}

Then, we associate a floating IP with the instance:

:::

::: {.cell .code}
```python
# run in Chameleon Jupyter environment
s.associate_floating_ip()
```
:::

::: {.cell .markdown}

In the output below, make a note of the floating IP assigned to the instance (in the "Addresses" row).

:::

::: {.cell .code}
```python
# run in Chameleon Jupyter environment
s.refresh()
s.show(type="widget")
```
:::

::: {.cell .markdown}

By default, all connections to VM resources are blocked as a security measure. We need to attach one or more security groups to permit access over the Internet on specific ports.

The following security groups will be created (if they do not already exist in our project) and then added to our server. These include SSH, Jupyter, and the user-facing dashboards we will access throughout the lab:

:::

::: {.cell .code}
```python
# run in Chameleon Jupyter environment
security_groups = [
    {"name": "allow-ssh", "port": 22, "description": "Enable SSH traffic on TCP port 22"},
    {"name": "allow-8888", "port": 8888, "description": "Enable TCP port 8888 (Jupyter)"},
    {"name": "allow-8000", "port": 8000, "description": "Enable TCP port 8000 (FastAPI docs)"},
    {"name": "allow-8080", "port": 8080, "description": "Enable TCP port 8080 (Airflow UI)"},
    {"name": "allow-5050", "port": 5050, "description": "Enable TCP port 5050 (Adminer UI)"},
    {"name": "allow-8090", "port": 8090, "description": "Enable TCP port 8090 (Redpanda Console)"},
    {"name": "allow-8081", "port": 8081, "description": "Enable TCP port 8081 (Redis Insight)"},
    {"name": "allow-9001", "port": 9001, "description": "Enable TCP port 9001 (MinIO Console)"},
    {"name": "allow-3000", "port": 3000, "description": "Enable TCP port 3000 (Nimtable UI)"},
]
```
:::

::: {.cell .code}
```python
# run in Chameleon Jupyter environment
for sg in security_groups:
    secgroup = network.SecurityGroup(
        {
            "name": sg["name"],
            "description": sg["description"],
        }
    )
    secgroup.add_rule(direction="ingress", protocol="tcp", port=sg["port"])
    secgroup.submit(idempotent=True)
    s.add_security_group(sg["name"])

print(f"updated security groups: {[sg['name'] for sg in security_groups]}")
```
:::

::: {.cell .code}
```python
# run in Chameleon Jupyter environment
s.refresh()
s.check_connectivity()
```
:::

::: {.cell .markdown}

### Retrieve code and notebooks on the instance

Now, we can use `python-chi` to execute commands on the instance to set it up. We will start by retrieving the lab code and materials on the instance.

:::

::: {.cell .code}
```python
# run in Chameleon Jupyter environment
s.execute("git clone https://github.com/teaching-on-testbeds/data-platform-chi")
```
:::

::: {.cell .markdown}

### Set up Docker

Here, we install Docker on the instance.

:::

::: {.cell .code}
```python
# run in Chameleon Jupyter environment
s.execute("curl -sSL https://get.docker.com/ | sudo sh")
s.execute("sudo groupadd -f docker; sudo usermod -aG docker $USER")
```
:::

::: {.cell .markdown}

## Open an SSH session

Finally, open an SSH session on the server. From your local terminal, run:

```
ssh -i ~/.ssh/id_rsa_chameleon cc@A.B.C.D
```

where:

* in place of `~/.ssh/id_rsa_chameleon`, substitute the path to your own key uploaded to KVM@TACC
* in place of `A.B.C.D`, use the floating IP address associated with your instance.

:::
