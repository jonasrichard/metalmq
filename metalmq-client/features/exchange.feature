Feature: Exchange

    Scenario: Send a message to an exchange
        Given an exchange declared as messages
        When a user sends a message
        Then the other user will get it
