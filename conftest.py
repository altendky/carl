"""Register CI manifests after pytest-cov starts measuring application imports."""

pytest_plugins = ("carl._tests.ci_shards",)
