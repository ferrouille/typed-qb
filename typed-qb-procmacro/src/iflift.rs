use syn::{token::Semi, Block, Expr, ExprBlock, ExprIf, Item, Local, Stmt};

#[derive(Debug)]
pub struct ConditionTree {
    inner: Vec<ConditionItem>,
}

impl ConditionTree {
    pub fn new() -> Self {
        ConditionTree { inner: Vec::new() }
    }

    pub fn cond(&mut self, cond: Expr, left: ConditionTree, right: Option<ConditionTree>) {
        self.inner.push(ConditionItem { cond, left, right })
    }

    pub fn create_apply_tree<'a>(
        &self,
        mut choices: &'a [bool],
        target: &mut Vec<ApplyItem>,
    ) -> &'a [bool] {
        for item in self.inner.iter() {
            choices = item.create_apply_tree(choices, target);
        }

        choices
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
    cond: Expr,
    left: ConditionTree,
    right: Option<ConditionTree>,
}

impl ConditionItem {
    pub fn create_apply_tree<'a>(
        &self,
        choices: &'a [bool],
        target: &mut Vec<ApplyItem>,
    ) -> &'a [bool] {
        let (choice, choices) = choices.split_first().unwrap();

        target.push(ApplyItem {
            cond_is_true: *choice,
        });

        if *choice {
            self.left.create_apply_tree(choices, target)
        } else {
            if let Some(right) = &self.right {
                right.create_apply_tree(choices, target)
            } else {
                choices
            }
        }
    }

    pub fn create_binary_tree(&self, index: usize) -> (usize, BinaryConditionTree) {
        let original_index = index;
        let (index, left) = self.left.create_binary_tree(index + 1);
        let (index, right) = self
            .right
            .as_ref()
            .map(|t| t.create_binary_tree(index))
            .unwrap_or((index, BinaryConditionTree::Leaf));

        (
            index,
            BinaryConditionTree::Node {
                index: original_index,
                cond: self.cond.clone(),
                left: Box::new(left),
                right: Box::new(right),
            },
        )
    }
}

#[derive(Debug, Clone)]
pub enum BinaryConditionTree {
    Node {
        index: usize,
        cond: Expr,
        left: Box<BinaryConditionTree>,
        right: Box<BinaryConditionTree>,
    },
    Leaf,
}

impl BinaryConditionTree {
    pub fn expand_leaves(&mut self, new_leaf: &BinaryConditionTree) {
        match self {
            BinaryConditionTree::Node { left, right, .. } => {
                left.expand_leaves(new_leaf);
                right.expand_leaves(new_leaf);
            }
            BinaryConditionTree::Leaf => *self = new_leaf.clone(),
        }
    }

    pub fn codegen(
        &self,
        choices: &mut [bool],
        source: &Block,
        condition_tree: &ConditionTree,
    ) -> Expr {
        match self {
            BinaryConditionTree::Node {
                index,
                cond,
                left,
                right,
            } => {
                choices[*index] = true;
                let then_branch = left.codegen(choices, source, condition_tree);

                choices[*index] = false;
                let else_branch = right.codegen(choices, source, condition_tree);

                Expr::If(ExprIf {
                    attrs: Vec::new(),
                    if_token: Default::default(),
                    cond: Box::new(cond.clone()),
                    then_branch: Block {
                        brace_token: Default::default(),
                        stmts: vec![Stmt::Expr(then_branch)],
                    },
                    else_branch: Some((Default::default(), Box::new(else_branch))),
                })
            }
            BinaryConditionTree::Leaf => {
                let mut result = source.clone();
                let mut apply_tree = Vec::new();
                condition_tree.create_apply_tree(choices, &mut apply_tree);
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
    cond_is_true: bool,
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

                c.cond(e.cond.as_ref().clone(), then_conds, else_conds);
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
                for arm in e.arms.iter() {
                    arm.body.extract_conditions(c);
                }
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
                if first.cond_is_true {
                    c = e.then_branch.apply_conditions(rest);
                    *self = Expr::Block(ExprBlock {
                        attrs: Vec::new(),
                        label: None,
                        block: e.then_branch.clone(),
                    });
                } else {
                    *self = e
                        .else_branch
                        .as_ref()
                        .map(|(_, e)| {
                            let mut e = e.as_ref().clone();
                            c = e.apply_conditions(rest);
                            e
                        })
                        .expect("Unfolding ifs without else branches is currently not supported");
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
                c = e.expr.apply_conditions(c);
                for arm in e.arms.iter_mut() {
                    c = arm.body.apply_conditions(c);
                }

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
