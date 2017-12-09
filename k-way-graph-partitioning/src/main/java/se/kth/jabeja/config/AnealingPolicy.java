package se.kth.jabeja.config;

public enum AnealingPolicy {
    LINEAR("LINEAR"),
    ACPROP("ACPROP");
    String name;
    AnealingPolicy(String name) {
        this.name = name;
    }
    @Override
    public String toString() {
        return name;
    }
}
