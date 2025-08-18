#include <clang-tidy/ClangTidy.h>
#include <clang-tidy/ClangTidyCheck.h>
#include <clang-tidy/ClangTidyModule.h>
#include <clang-tidy/ClangTidyModuleRegistry.h>
#include <clang/AST/ASTContext.h>
#include <clang/ASTMatchers/ASTMatchFinder.h>

using namespace clang;
using namespace clang::tidy;
using namespace clang::ast_matchers;

// Declare the anchor to ensure linkage
extern volatile int AwesomePrefixCheckModuleAnchor;

class AwesomePrefixCheck : public ClangTidyCheck {
public:
    AwesomePrefixCheck(StringRef Name, ClangTidyContext* Context)
      : ClangTidyCheck(Name, Context) {}
    void registerMatchers(ast_matchers::MatchFinder* Finder) override;
    void check(const ast_matchers::MatchFinder::MatchResult& Result) override;
};

void AwesomePrefixCheck::registerMatchers(MatchFinder* Finder) {
    Finder->addMatcher(functionDecl().bind("add_awesome_prefix"), this);
}

void AwesomePrefixCheck::check(const MatchFinder::MatchResult& Result) {
    const auto* MatchedDecl = Result.Nodes.getNodeAs<FunctionDecl>(
      "add_awesome_prefix");
    if (
      !MatchedDecl->getIdentifier()
      || MatchedDecl->getName().starts_with("awesome_")) {
        return;
    }
    diag(MatchedDecl->getLocation(), "function %0 is insufficiently awesome")
      << MatchedDecl;
    diag(MatchedDecl->getLocation(), "insert 'awesome'", DiagnosticIDs::Note)
      << FixItHint::CreateInsertion(MatchedDecl->getLocation(), "awesome_");
}

namespace {

class AwesomePrefixCheckModule : public ClangTidyModule {
public:
    void addCheckFactories(ClangTidyCheckFactories& CheckFactories) override {
        CheckFactories.registerCheck<AwesomePrefixCheck>(
          "redpanda-example-check");
    }
};

} // namespace

// Register the module using this statically initialized variable.
static ClangTidyModuleRegistry::Add<::AwesomePrefixCheckModule>
  X("redpanda", "Add redpanda checks.");

// This anchor is used to force the linker to link in the generated object file
// and thus register the module.
volatile int AwesomePrefixCheckModuleAnchor = 0;

// Force reference to the anchor to ensure it gets linked
__attribute__((constructor)) void ForceReference() {
  (void)AwesomePrefixCheckModuleAnchor;
}
