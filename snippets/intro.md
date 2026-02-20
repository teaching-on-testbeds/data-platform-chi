
::: {.cell .markdown}

# Data platforms on Chameleon

You are an engineer at GourmetGram, a startup where users upload food photos, write captions, and interact through views, comments, and flags. GourmetGram is in a major growth phase, thanks in part to our previous infrastructure and MLOps work.

Now our manager wants to expand the platform with more ML features, and has suggested content moderation as a strong first use case for a new model. To make that possible, we need to build a data platform that supports both real-time decisions and reliable training data.

In this lab, we will build that platform progressively:

* First, we will use PostgreSQL to store core application state for users, images, comments, and flags.
* Then, we will expose API endpoints and run synthetic traffic generation so the system has realistic activity data.
* Next, we will add a real-time pipeline for streaming events and moderation triggers.
* After that, we will add a batch lakehouse layer with Iceberg tables for durable training data.
* Finally, we will train and deploy a moderation model and compare model-based decisions with heuristic decisions.

To run this lab, we should already have a Chameleon account, be part of a project, and have an SSH key uploaded to KVM@TACC.

Before we begin, open this experiment artifact on Trovi:

* [Data platforms on Chameleon (Trovi artifact)](https://trovi.chameleoncloud.org/dashboard/artifacts/3aca301f-22f7-4929-88c4-4b666f3d4c92)

Then follow along with the published lab page:

* [Data platforms on Chameleon](https://teaching-on-testbeds.github.io/data-platform-chi/)

:::
