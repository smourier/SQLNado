using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Well, sometimes, you have no choice.")]
[assembly: SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Kept for compat reasons.")]
[assembly: SuppressMessage("SonarQube", "S2346:Flags enumerations zero-value members should be named \"None\"", Justification = "Ridiculous, we use interop a lot.")]
[assembly: SuppressMessage("SonarQube", "S1144:Unused private types or members should be removed", Justification = "Ridiculous, we use interop a lot.")]
[assembly: SuppressMessage("SonarQube", "S4487:Unread \"private\" fields should be removed", Justification = "Ridiculous, we use interop a lot.")]
[assembly: SuppressMessage("SonarQube", "S4214:\"P/Invoke\" methods should not be visible", Justification = "Ridiculous, we use interop a lot.")]
[assembly: SuppressMessage("SonarQube", "S4070:Non-flags enums should not be marked with \"FlagsAttribute\"", Justification = "Ridiculous, we use interop a lot.")]
[assembly: SuppressMessage("SonarQube", "S4200:Native methods should be wrapped", Justification = "Ridiculous, we use interop a lot.")]
[assembly: SuppressMessage("SonarQube", "S3776:Cognitive Complexity of methods should not be too high", Justification = "Don't want to change the logic.")]
[assembly: SuppressMessage("SonarQube", "S1699:Constructors should only call non-overridable methods", Justification = "Don't want to change the logic.")]
[assembly: SuppressMessage("SonarQube", "S2223:Non-constant static fields should not be visible", Justification = "It's fine as it is.")]
[assembly: SuppressMessage("SonarQube", "S107:Methods should not have too many parameters", Justification = "Talk to SQLite devs")]
