let context = #{};


// Use the same switches, defaults, etc passed to this archetype, and pass them to child archetypes
let render_settings = archetect::archetype::settings();

// This generally comes from ANSWERS, but prompting for it ensures it is in the context 
context += prompt("Project Author:", "author_full", #{
    placeholder: "Your Name <your.name@example.com>",
});

context += render(Archetype("org-prompts"), ANSWERS, render_settings);

context += prompt("Project Name:", "project-name", #{
  cases: [
    CasedValue(KebabCase), 
  ], 
});

render(Directory("contents"), context);

//
// Publishing
//
if switch_enabled("zip") {
    zip(context["project-name"], render("{{ project-name }}.zip", context));
}
if switch_enabled("tarball") {
    tar_gz(context["project-name"], render("{{ project-name }}.tar.gz", context));
}

if switch_enabled("git-init") || switch_enabled("git-publish") {
    git_init(context["project-name"], "main");
    git_add_all(context["project-name"]);
    git_commit(context["project-name"], "initial commit");
    git_remote_add(context["project-name"], "origin", render("https://github.com/{{ org-solution-name }}/{{ project-name }}.git", context));

    if switch_enabled("git-publish") {
        gh_repo_create(render("{{ org-solution-name }}/{{ project-name }}", context), Private);
        git_push(context["project-name"], "origin", "main");
    }
} else {
    display();

    log(Info, "Copy and paste the following commands to publish your project:");

    display();

    display(render("cd {{ project-name }}", context));
    display("git init -b main");
    display(render("gh repo create {{ org-solution-name }}/{{ project-name }} --private --source=. --remote=origin", context));
    display("git add .");
    display("git commit -m 'initial commit'");
    display("git push -u origin HEAD");
}

context
