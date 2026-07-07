# Development Environment Setup
# Manages installation and updates for required tools via Homebrew

.PHONY: brew-common brew-update-tools brew-setup-all setup-help

# List of tools to be installed/managed
BREW_TOOLS := k3d kubectl kustomize k9s
BREW_TAPS := fluxcd/tap/flux

# Help for setup commands
setup-help:
	@echo "Setup Commands:"
	@echo "--------------"
	@echo "  brew-common         - Install required development tools"
	@echo "  brew-update-tools   - Update all installed tools"
	@echo "  brew-setup-all      - Install and update all tools"
	@echo ""

# Install required development tools
brew-common:
	@echo "==> Installing required tools..."
	@brew update
	@for tool in $(BREW_TOOLS); do \
		echo "  - Checking $$tool"; \
		brew list $$tool &>/dev/null || brew install $$tool; \
	done
	@for tap in $(BREW_TAPS); do \
		echo "  - Checking $$tap"; \
		brew list $$tap &>/dev/null || brew install $$tap; \
	done
	@echo "==> Installation complete"

# Update all installed tools
brew-update-tools:
	@echo "==> Updating installed tools..."
	@brew update
	@for tool in $(BREW_TOOLS); do \
		if brew list $$tool &>/dev/null; then \
			echo "  - Upgrading $$tool"; \
			brew upgrade $$tool; \
		else \
			echo "  - $$tool not installed, skipping"; \
		fi \
	done
	@for tap in $(BREW_TAPS); do \
		if brew list $$tap &>/dev/null; then \
			echo "  - Upgrading $$tap"; \
			brew upgrade $$tap; \
		else \
			echo "  - $$tap not installed, skipping"; \
		fi \
	done
	@echo "==> Update complete"

# Install or update all tools
brew-setup-all: brew-common brew-update-tools
	@echo "==> Environment setup complete"
