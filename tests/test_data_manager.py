import pytest
import json
import os
from data_manager import PortfolioManager

def test_portfolio_manager_is_in_depot(tmp_path):
    # Erstelle eine temporäre Portfolio-Datei
    portfolio_file = tmp_path / "portfolio.json"
    test_data = [
        {"Yahoo_Symbol": "AAPL", "Name": "Apple"},
        {"Yahoo_Symbol": "MSFT", "Name": "Microsoft"}
    ]
    with open(portfolio_file, "w", encoding="utf-8") as f:
        json.dump(test_data, f)
    
    pm = PortfolioManager(str(portfolio_file))
    
    # Teste Symbole
    assert pm.is_in_depot("AAPL") is True
    assert pm.is_in_depot("MSFT") is True
    assert pm.is_in_depot("TSLA") is False
    
    # Teste Case-Insensitivity
    assert pm.is_in_depot("aapl") is True
    assert pm.is_in_depot("  Msft  ") is True