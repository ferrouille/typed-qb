use proc_macro2::Span;
use syn::{
    token::{If, Semi},
    Arm, Block, Expr, ExprBlock, ExprLit, ExprMatch, Item, Lit, LitBool, Local, Pat, PatLit, Stmt,
};

#[derive(Debug)]
pub struct ConditionTree {
    inner: Vec<ConditionItem>,
}

impl ConditionTree {
    pub fn new() -> Self {
        ConditionTree { inner: Vec::new() }
    }

    pub fn cond(
        &mut self,
        cond: Expr,
        branches: Vec<(Pat, Option<(If, Box<Expr>)>, ConditionTree)>,
    ) {
        self.inner.push(ConditionItem {
            expr: cond,
            branches,
        })
    }

    pub fn create_apply_tree<'a>(
        &self,
        mut choices: &'a [usize],
        target: &mut Vec<ApplyItem>,
    ) -> Result<&'a [usize], BranchOverflow> {
        for item in self.inner.iter() {
            choices = item.create_apply_tree(choices, target)?;
        }

        Ok(choices)
    }

    pub fn create_binary_tree(&self, mut index: usize) -> (usize, BinaryConditionTree) {
        let mut v = Vec::new();
        for item in self.inner.iter() {
            let (new_index, t) = item.create_binary_tree(index);
            index = new_index;
            v.push(t);
        }

        (
            index,
            v.into_iter()
                .reduce(|mut acc, el| {
                    acc.expand_leaves(&el);
                    acc
                })
                .unwrap_or(BinaryConditionTree::Leaf),
        )
    }
}

#[derive(Debug)]
pub struct ConditionItem {
    expr: Expr,
    branches: Vec<(Pat, Option<(If, Box<Expr>)>, ConditionTree)>,
}

pub struct BranchOverflow(usize);

impl ConditionItem {
    pub fn create_apply_tree<'a>(
        &self,
        choices: &'a [usize],
        target: &mut Vec<ApplyItem>,
    ) -> Result<&'a [usize], BranchOverflow> {
        let (choice, choices) = choices.split_first().unwrap();

        target.push(ApplyItem {
            branch_taken: *choice,
        });

        match self.branches.get(*choice) {
            Some((_, _, branch)) => Ok(branch.create_apply_tree(choices, target)?),
            None => Err(BranchOverflow(choices.len())),
        }
    }

    pub fn create_binary_tree(&self, mut index: usize) -> (usize, BinaryConditionTree) {
        let original_index = index;
        index += 1;

        let mut branches = Vec::new();
        for (pat, guard, branch) in self.branches.iter() {
            let (new_index, new_branch) = branch.create_binary_tree(index);
            index = new_index;
            branches.push((pat.clone(), guard.clone(), new_branch));
        }

        (
            index,
            BinaryConditionTree::Node {
                index: original_index,
                expr: self.expr.clone(),
                branches,
            },
        )
    }
}

#[derive(Debug, Clone)]
pub enum BinaryConditionTree {
    Node {
        index: usize,
        expr: Expr,
        branches: Vec<(Pat, Option<(If, Box<Expr>)>, BinaryConditionTree)>,
    },
    Leaf,
}

impl BinaryConditionTree {
    pub fn expand_leaves(&mut self, new_leaf: &BinaryConditionTree) {
        match self {
            BinaryConditionTree::Node { branches, .. } => {
                for (_, _, branch) in branches.iter_mut() {
                    branch.expand_leaves(new_leaf);
                }
            }
            BinaryConditionTree::Leaf => *self = new_leaf.clone(),
        }
    }

    pub fn codegen(
        &self,
        choices: &mut [usize],
        source: &Block,
        condition_tree: &ConditionTree,
    ) -> Expr {
        match self {
            BinaryConditionTree::Node {
                index,
                expr,
                branches,
            } => {
                let mut arms = Vec::new();
                for (n, (pat, guard, branch)) in branches.iter().enumerate() {
                    choices[*index] = n;
                    let branch = branch.codegen(choices, source, condition_tree);
                    arms.push(Arm {
                        attrs: Vec::new(),
                        pat: pat.clone(),
                        guard: guard.clone(),
                        fat_arrow_token: Default::default(),
                        body: Box::new(branch),
                        comma: Default::default(),
                    });
                }

                Expr::Match(ExprMatch {
                    attrs: Vec::new(),
                    match_token: Default::default(),
                    expr: Box::new(expr.clone()),
                    brace_token: Default::default(),
                    arms,
                })
            }
            BinaryConditionTree::Leaf => {
                let mut result = source.clone();
                let mut apply_tree = Vec::new();
                condition_tree
                    .create_apply_tree(choices, &mut apply_tree)
                    .unwrap_or_else(|_| unreachable!());
                let remaining = result.apply_conditions(&apply_tree);
                assert!(remaining.is_empty());

                Expr::Block(ExprBlock {
                    attrs: Vec::new(),
                    label: None,
                    block: result,
                })
            }
        }
    }
}

#[derive(Debug)]
pub struct ApplyItem {
    branch_taken: usize,
}

pub trait IfLifting {
    fn extract_conditions(&self, c: &mut ConditionTree);

    #[must_use]
    fn apply_conditions<'a>(&mut self, c: &'a [ApplyItem]) -> &'a [ApplyItem];
}

impl IfLifting for Block {
    fn extract_conditions(&self, c: &mut ConditionTree) {
        for s in self.stmts.iter() {
            s.extract_conditions(c);
        }
    }

    fn apply_conditions<'a>(&mut self, mut c: &'a [ApplyItem]) -> &'a [ApplyItem] {
        for s in self.stmts.iter_mut() {
            c = s.apply_conditions(c);
        }

        c
    }
}

impl IfLifting for Stmt {
    fn extract_conditions(&self, c: &mut ConditionTree) {
        match self {
            Stmt::Local(x) => x.extract_conditions(c),
            Stmt::Item(x) => x.extract_conditions(c),
            Stmt::Expr(x) => x.extract_conditions(c),
            Stmt::Semi(x, y) => {
                x.extract_conditions(c);
                y.extract_conditions(c);
            }
        }
    }

    fn apply_conditions<'a>(&mut self, mut c: &'a [ApplyItem]) -> &'a [ApplyItem] {
        match self {
            Stmt::Local(x) => x.apply_conditions(c),
            Stmt::Item(x) => x.apply_conditions(c),
            Stmt::Expr(x) => x.apply_conditions(c),
            Stmt::Semi(x, y) => {
                c = x.apply_conditions(c);
                y.apply_conditions(c)
            }
        }
    }
}

impl IfLifting for Local {
    fn extract_conditions(&self, c: &mut ConditionTree) {
        if let Some((_, init)) = &self.init {
            init.extract_conditions(c)
        }
    }

    fn apply_conditions<'a>(&mut self, mut c: &'a [ApplyItem]) -> &'a [ApplyItem] {
        if let Some((_, init)) = &mut self.init {
            c = init.apply_conditions(c);
        }

        c
    }
}

impl IfLifting for Item {
    fn extract_conditions(&self, _: &mut ConditionTree) {
        todo!()
    }

    fn apply_conditions<'a>(&mut self, c: &'a [ApplyItem]) -> &'a [ApplyItem] {
        c
    }
}

impl IfLifting for Expr {
    fn extract_conditions(&self, c: &mut ConditionTree) {
        match self {
            Expr::Array(e) => {
                for el in e.elems.iter() {
                    el.extract_conditions(c);
                }
            }
            Expr::Assign(e) => {
                e.left.extract_conditions(c);
                e.right.extract_conditions(c);
            }
            Expr::AssignOp(e) => {
                e.left.extract_conditions(c);
                e.right.extract_conditions(c);
            }
            Expr::Async(e) => {
                e.block.extract_conditions(c);
            }
            Expr::Await(e) => {
                e.base.extract_conditions(c);
            }
            Expr::Binary(e) => {
                e.left.extract_conditions(c);
                e.right.extract_conditions(c);
            }
            Expr::Block(e) => {
                e.block.extract_conditions(c);
            }
            Expr::Box(e) => {
                e.expr.extract_conditions(c);
            }
            Expr::Break(e) => {
                if let Some(e) = &e.expr {
                    e.extract_conditions(c);
                }
            }
            Expr::Call(e) => {
                e.func.extract_conditions(c);
                for arg in e.args.iter() {
                    arg.extract_conditions(c);
                }
            }
            Expr::Cast(e) => {
                e.expr.extract_conditions(c);
            }
            Expr::Closure(e) => {
                e.body.extract_conditions(c);
            }
            Expr::Continue(_) => {}
            Expr::Field(e) => {
                e.base.extract_conditions(c);
            }
            Expr::ForLoop(e) => {
                e.expr.extract_conditions(c);
                e.body.extract_conditions(c);
            }
            Expr::Group(e) => {
                e.expr.extract_conditions(c);
            }
            Expr::If(e) => {
                let mut then_conds = ConditionTree::new();
                e.then_branch.extract_conditions(&mut then_conds);
                let else_conds = e.else_branch.as_ref().map(|(_, e)| {
                    let mut conds = ConditionTree::new();
                    e.extract_conditions(&mut conds);
                    conds
                });

                let mut branches = vec![(
                    Pat::Lit(PatLit {
                        attrs: Vec::new(),
                        expr: Box::new(Expr::Lit(ExprLit {
                            attrs: Vec::new(),
                            lit: Lit::Bool(LitBool::new(true, Span::call_site())),
                        })),
                    }),
                    None,
                    then_conds,
                )];

                if let Some(else_conds) = else_conds {
                    branches.push((
                        Pat::Lit(PatLit {
                            attrs: Vec::new(),
                            expr: Box::new(Expr::Lit(ExprLit {
                                attrs: Vec::new(),
                                lit: Lit::Bool(LitBool::new(false, Span::call_site())),
                            })),
                        }),
                        None,
                        else_conds,
                    ));
                }

                c.cond(e.cond.as_ref().clone(), branches);
            }
            Expr::Index(e) => {
                e.expr.extract_conditions(c);
                e.index.extract_conditions(c);
            }
            Expr::Let(e) => {
                e.expr.extract_conditions(c);
            }
            Expr::Loop(e) => {
                e.body.extract_conditions(c);
            }
            Expr::Match(e) => {
                e.expr.extract_conditions(c);

                let mut branches = Vec::new();
                for arm in e.arms.iter() {
                    let mut branch = ConditionTree::new();
                    arm.body.extract_conditions(&mut branch);
                    branches.push((arm.pat.clone(), arm.guard.clone(), branch));
                }

                c.cond(e.expr.as_ref().clone(), branches);
            }
            Expr::MethodCall(e) => {
                e.receiver.extract_conditions(c);
                for arg in e.args.iter() {
                    arg.extract_conditions(c);
                }
            }
            Expr::Paren(e) => {
                e.expr.extract_conditions(c);
            }
            Expr::Range(e) => {
                if let Some(e) = &e.from {
                    e.extract_conditions(c);
                }

                if let Some(e) = &e.to {
                    e.extract_conditions(c);
                }
            }
            Expr::Reference(e) => {
                e.expr.extract_conditions(c);
            }
            Expr::Repeat(e) => {
                e.len.extract_conditions(c);
                e.expr.extract_conditions(c);
            }
            Expr::Return(e) => {
                if let Some(e) = &e.expr {
                    e.extract_conditions(c);
                }
            }
            Expr::Struct(e) => {
                for field in e.fields.iter() {
                    field.expr.extract_conditions(c);
                }
            }
            Expr::Try(e) => {
                e.expr.extract_conditions(c);
            }
            Expr::TryBlock(e) => {
                e.block.extract_conditions(c);
            }
            Expr::Tuple(e) => {
                for el in e.elems.iter() {
                    el.extract_conditions(c);
                }
            }
            Expr::Unary(e) => {
                e.expr.extract_conditions(c);
            }
            Expr::Unsafe(e) => {
                e.block.extract_conditions(c);
            }
            Expr::Verbatim(_) => {}
            Expr::While(e) => {
                e.cond.extract_conditions(c);
                e.body.extract_conditions(c);
            }
            Expr::Yield(e) => {
                if let Some(e) = &e.expr {
                    e.extract_conditions(c);
                }
            }
            Expr::Path(_) => {}
            Expr::Type(_) => {}
            Expr::Lit(_) => {}
            Expr::Macro(_) => {}
            _ => {
                // TODO: Compiler error because we can't lift all ifs
            }
        }
    }

    fn apply_conditions<'a>(&mut self, mut c: &'a [ApplyItem]) -> &'a [ApplyItem] {
        match self {
            Expr::Array(e) => {
                for el in e.elems.iter_mut() {
                    c = el.apply_conditions(c);
                }

                c
            }
            Expr::Assign(e) => {
                c = e.left.apply_conditions(c);
                e.right.apply_conditions(c)
            }
            Expr::AssignOp(e) => {
                c = e.left.apply_conditions(c);
                e.right.apply_conditions(c)
            }
            Expr::Async(e) => e.block.apply_conditions(c),
            Expr::Await(e) => e.base.apply_conditions(c),
            Expr::Binary(e) => {
                c = e.left.apply_conditions(c);
                e.right.apply_conditions(c)
            }
            Expr::Block(e) => e.block.apply_conditions(c),
            Expr::Box(e) => e.expr.apply_conditions(c),
            Expr::Break(e) => {
                if let Some(e) = &mut e.expr {
                    c = e.apply_conditions(c);
                }

                c
            }
            Expr::Call(e) => {
                c = e.func.apply_conditions(c);
                for arg in e.args.iter_mut() {
                    c = arg.apply_conditions(c);
                }

                c
            }
            Expr::Cast(e) => e.expr.apply_conditions(c),
            Expr::Closure(e) => e.body.apply_conditions(c),
            Expr::Field(e) => e.base.apply_conditions(c),
            Expr::ForLoop(e) => {
                c = e.expr.apply_conditions(c);
                e.body.apply_conditions(c)
            }
            Expr::Group(e) => e.expr.apply_conditions(c),
            Expr::If(e) => {
                let (first, rest) = c.split_first().unwrap();
                match first.branch_taken {
                    0 => {
                        c = e.then_branch.apply_conditions(rest);
                        *self = Expr::Block(ExprBlock {
                            attrs: Vec::new(),
                            label: None,
                            block: e.then_branch.clone(),
                        });
                    }
                    1 => {
                        *self = e
                            .else_branch
                            .as_ref()
                            .map(|(_, e)| {
                                let mut e = e.as_ref().clone();
                                c = e.apply_conditions(rest);
                                e
                            })
                            .expect("Else branch cannot be taken if it does not exist");
                    }
                    _ => unreachable!("branch_taken must be 0 or 1 for an if statement"),
                }

                c
            }
            Expr::Index(e) => {
                c = e.expr.apply_conditions(c);
                e.index.apply_conditions(c)
            }
            Expr::Let(e) => e.expr.apply_conditions(c),
            Expr::Loop(e) => e.body.apply_conditions(c),
            Expr::Match(e) => {
                let (first, rest) = c.split_first().unwrap();
                let arm = &mut e.arms[first.branch_taken];

                c = arm.body.apply_conditions(rest);
                *self = arm.body.as_ref().clone();

                c
            }
            Expr::MethodCall(e) => {
                c = e.receiver.apply_conditions(c);
                for arg in e.args.iter_mut() {
                    c = arg.apply_conditions(c);
                }

                c
            }
            Expr::Paren(e) => e.expr.apply_conditions(c),
            Expr::Range(e) => {
                if let Some(e) = &mut e.from {
                    c = e.apply_conditions(c);
                }

                if let Some(e) = &mut e.to {
                    c = e.apply_conditions(c);
                }

                c
            }
            Expr::Reference(e) => e.expr.apply_conditions(c),
            Expr::Repeat(e) => {
                c = e.len.apply_conditions(c);
                e.expr.apply_conditions(c)
            }
            Expr::Return(e) => {
                if let Some(e) = &mut e.expr {
                    c = e.apply_conditions(c);
                }

                c
            }
            Expr::Struct(e) => {
                for field in e.fields.iter_mut() {
                    c = field.expr.apply_conditions(c);
                }

                c
            }
            Expr::Try(e) => e.expr.apply_conditions(c),
            Expr::TryBlock(e) => e.block.apply_conditions(c),
            Expr::Tuple(e) => {
                for el in e.elems.iter_mut() {
                    c = el.apply_conditions(c);
                }

                c
            }
            Expr::Unary(e) => e.expr.apply_conditions(c),
            Expr::Unsafe(e) => e.block.apply_conditions(c),
            Expr::While(e) => {
                c = e.cond.apply_conditions(c);
                e.body.apply_conditions(c)
            }
            Expr::Yield(e) => {
                if let Some(e) = &mut e.expr {
                    c = e.apply_conditions(c);
                }

                c
            }
            Expr::Continue(_)
            | Expr::Path(_)
            | Expr::Type(_)
            | Expr::Lit(_)
            | Expr::Macro(_)
            | Expr::Verbatim(_) => c,
            _ => {
                // TODO: Compiler error because we can't lift all ifs
                c
            }
        }
    }
}

impl IfLifting for Semi {
    fn extract_conditions(&self, _: &mut ConditionTree) {}

    fn apply_conditions<'a>(&mut self, c: &'a [ApplyItem]) -> &'a [ApplyItem] {
        c
    }
}
