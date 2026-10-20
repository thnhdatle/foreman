# Security Policy

## Supported versions

| Version | Supported |
|---|---|
| 0.1.x | Yes |
| < 0.1 | No |

## Reporting a vulnerability

Please do not open public issues, discussions, or pull requests for security vulnerabilities.

Report vulnerabilities privately through GitHub private vulnerability reporting:

1. Go to the [thnhdatle/foreman](https://github.com/thnhdatle/foreman) repository.
2. Open the **Security** tab.
3. Select **Report a vulnerability** and fill in the form.

Include the affected version, a description of the issue, steps or a proof of concept to
reproduce it, and the impact you expect.

We aim to acknowledge reports within 72 hours. We will keep you informed while we investigate,
agree on a disclosure timeline with you, and credit you in the advisory unless you prefer
otherwise.

## Scope notes

- The admin HTTP handler in the `foremanhttp` package has no built-in authentication or
  authorization. Applications that embed it must protect it themselves, for example behind
  authentication middleware, on an internal-only listener, or both. Exposing it without
  protection is a deployment issue, not a vulnerability in Foreman.
- The SQL function `foreman_insert_job` is declared `SECURITY INVOKER`. It runs with the
  privileges of the calling role and does not grant any access beyond what that role already
  has on the Foreman tables.
