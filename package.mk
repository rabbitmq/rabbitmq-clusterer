DEPS:=rabbitmq-server
# we use WITH_BROKER in order to easily get a distributed node.
# TODO Finish the testsuite; it is disabled for now.
# WITH_BROKER_TEST_COMMANDS:=clusterer_test:test(1000000)

define package_rules

.PHONY: dialyze
dialyze: $(EBIN_BEAMS) $(PACKAGE_DIR)/../rabbitmq-server/rabbit.plt
	dialyzer --plt $(PACKAGE_DIR)/../rabbitmq-server/rabbit.plt --no_native --fullpath $(EBIN_BEAMS)

.PHONY: $(PACKAGE_DIR)/../rabbitmq-server/rabbit.plt
$(PACKAGE_DIR)/../rabbitmq-server/rabbit.plt:
	$(MAKE) -C $(PACKAGE_DIR)/../rabbitmq-server rabbit.plt

endef
