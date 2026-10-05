@e2e
Feature: Access an ACP agent through Gate
  Clients should see a consistent task lifecycle across transports, retain their
  conversation after cancellation, and receive only the observations they request.

  Background:
    Given Gate is running with a local ACP agent

  Scenario: Discover a usable service on an automatically selected port
    When a client discovers Gate
    Then the card advertises Gate, streaming, and both observation extensions
    And every advertised interface points to the running service

  Scenario Outline: Report why an agent stopped
    When the agent stops with "<reason>"
    Then the task state is "<state>"

    Examples:
      | reason            | state     |
      | end_turn          | completed |
      | max_tokens        | failed    |
      | max_turn_requests | failed    |
      | refusal           | rejected  |
      | cancelled         | canceled  |

  Scenario: Continue a conversation with a new task
    When a client sends two messages in the same conversation
    Then the messages have distinct task IDs and the same ACP session
    And retrieving the latest task preserves its completed result

  Scenario: A tool cannot proceed without permission
    When the agent requests permission to run a tool
    Then the task state is "canceled"
    And the result contains only "Researching."

  Scenario: Agent failures do not expose private RPC details
    When the agent returns an RPC error containing private details
    Then the task state is "failed"
    And private agent details are absent from the task

  Scenario Outline: Receive only requested observations
    Given the client requests "<observations>" observations
    When the agent streams text, thoughts, and tool activity
    Then the client receives <thoughts> thoughts and <tools> tool observations
    And observations have increasing sequence numbers and usable IDs
    And private agent details are absent from the stream
    And the stream starts with a submitted task followed by working status
    And result chunks append to the same artifact
    And the completed result is "research complete" with no history or plans

    Examples:
      | observations | thoughts | tools |
      | none         | 0        | 0     |
      | thoughts     | 1        | 0     |
      | tools        | 0        | 2     |
      | both         | 1        | 2     |

  Scenario: Cancel a task and continue the conversation
    Given the agent is working on a task
    When the client cancels the task
    Then the task state is "canceled"
    And the same ACP session can complete another message

  Scenario: Disconnecting a stream does not cancel the work
    Given the agent is working on a task
    When the client disconnects the stream
    Then the saved task is still working
    When the client cancels the task
    Then the task state is "canceled"

  Scenario Outline: Receive the same result over each transport
    When a client sends a message over "<transport>"
    Then the task state is "completed"
    And the result contains only "research complete"

    Examples:
      | transport |
      | HTTP+JSON |
      | Connect   |
      | gRPC-Web  |
      | gRPC      |

  Scenario: Request a task that does not exist
    When a client retrieves an unknown task
    Then the service reports task not found
