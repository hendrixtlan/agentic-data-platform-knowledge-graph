from kg_lab.langgraph_app import agent


if __name__ == "__main__":
    question = "What systems depend on customer_events and why was Kafka selected?"
    result = agent.invoke({"question": question})
    print(result["answer"])
