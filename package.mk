DEPS:=rabbitmq-server
WITH_BROKER_TEST_COMMANDS:=clusterer_test:test()

define package_rules

.PHONY: dialyze
dialyze: $(EBIN_BEAMS) $(PACKAGE_DIR)/../rabbitmq-server/rabbit.plt
	dialyzer --plt $(PACKAGE_DIR)/../rabbitmq-server/rabbit.plt --no_native --fullpath $(EBIN_BEAMS)

.PHONY: $(PACKAGE_DIR)/../rabbitmq-server/rabbit.plt
$(PACKAGE_DIR)/../rabbitmq-server/rabbit.plt:
	$(MAKE) -C $(PACKAGE_DIR)/../rabbitmq-server rabbit.plt

endef
