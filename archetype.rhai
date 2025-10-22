let context = #{};

// This generally comes from ANSWERS, but prompting for it ensures it is in the context 
context += prompt("Project Author:", "author_full", #{
    placeholder: "Your Name <your.name@example.com>",
});

context += prompt("Project Name:", "archetype-name", #{
  cases: [
    CasedValue(KebabCase), 
  ], 
});

context += prompt("Greeting:", "greeting", #{
  defaults_with: "Hello, World!",
  help: "Cliche greeting",
});

render(Directory("contents"), context)
