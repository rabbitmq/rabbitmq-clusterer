PROJECT = rabbitmq_clusterer
PROJECT_VERSION =
PROJECT_DESCRIPTION = Declarative RabbitMQ clustering
PROJECT_MOD = rabbit_clusterer
PROJECT_APP_EXTRA_KEYS = {broker_version_requirements, ["3.6.0", "3.7.0"]}

BUILD_DEPS = rabbit_common rabbit

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk

# --------------------------------------------------------------------
# Testing.
# --------------------------------------------------------------------

# clusterer test suite was never finished
# and currently disabled
