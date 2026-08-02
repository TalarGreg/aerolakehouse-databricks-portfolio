# Security Policy

## Reporting a Vulnerability

Please use GitHub private vulnerability reporting when it is enabled for this
repository. Do not include credentials, tokens, personal data, or exploitable
details in a public issue.

If private reporting is unavailable, open a public issue containing only a
high-level description and request a private communication channel.

## Secrets and Runtime Data

This repository must not contain credentials, Terraform state, plan files,
Databricks profiles, environment files, or exported production data. Runtime
secrets belong in Databricks Secret Scope or protected GitHub environments.

If a credential is committed or printed in a workflow log, revoke it first and
then remove the exposed material. Rewriting Git history is not a substitute for
credential rotation.

## Supported Version

Security fixes are applied to the latest commit on `main`.
