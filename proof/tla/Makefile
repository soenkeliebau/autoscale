TLA2TOOLS := tla2tools.jar
JAVA := java
TLC_OPTS := -workers auto -deadlock
TLA2TOOLS_URL := https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar

.PHONY: translate check-framework check-nifi check-toctou check-all simulate clean download-tools

download-tools:
	@test -f $(TLA2TOOLS) || curl -L -o $(TLA2TOOLS) $(TLA2TOOLS_URL)

translate: download-tools StackableScalerFramework.tla
	$(JAVA) -cp $(TLA2TOOLS) pcal.trans StackableScalerFramework.tla

check-framework: download-tools StackableScalerFramework.tla StackableScalerFramework.cfg
	$(JAVA) -jar $(TLA2TOOLS) -config StackableScalerFramework.cfg $(TLC_OPTS) StackableScalerFramework.tla

check-nifi: download-tools NiFiScalerRefinement.tla NiFiScalerRefinement.cfg
	$(JAVA) -jar $(TLA2TOOLS) -config NiFiScalerRefinement.cfg $(TLC_OPTS) NiFiScalerRefinement.tla

check-toctou: download-tools StackableScalerFramework.tla TOCTOU.cfg
	@echo "--- TOCTOU Race Detection ---"
	$(JAVA) -jar $(TLA2TOOLS) -config TOCTOU.cfg $(TLC_OPTS) StackableScalerFramework.tla

check-all: check-framework check-nifi

simulate: download-tools StackableScalerFramework.tla StackableScalerFramework.cfg
	$(JAVA) -jar $(TLA2TOOLS) -simulate -depth 100 -config StackableScalerFramework.cfg StackableScalerFramework.tla

clean:
	rm -rf states/
